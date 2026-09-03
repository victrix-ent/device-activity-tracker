import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import { Pool } from 'pg';
import pino from 'pino';
import makeWASocket, {
    DisconnectReason,
} from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';

import {
    createDatabaseAuthState,
} from './baileys-db-auth.js';

import {
    WhatsAppTracker,
} from './tracker.js';


/* ============================================================
   SERVER
   ============================================================ */

const PORT = Number(process.env.PORT || 3000);

const app = express();

const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
    },
});


/* ============================================================
   DATABASE
   ============================================================ */

const databaseUrl = process.env.DATABASE_URL;

const pool = databaseUrl
    ? new Pool({
        connectionString: databaseUrl,
        ssl: {
            rejectUnauthorized: false,
        },
    })
    : null;

if (!pool) {
    console.error(
        '[DB] DATABASE_URL is not configured',
    );
} else {
    pool.on('error', (err) => {
        console.error(
            '[DB] Unexpected PostgreSQL pool error:',
            err,
        );
    });
}


/* ============================================================
   TYPES
   ============================================================ */

type TrackerPlatform =
    | 'whatsapp'
    | 'signal';

interface TrackedContact {
    id: string;
    platform: TrackerPlatform;
    phoneNumber: string;
}

interface Measurement {
    deviceId: string;
    rtt: number | null;
    avg: number | null;
    median: number | null;
    threshold: number | null;
    state: string | null;
    measuredAt?: string;
}


/* ============================================================
   WHATSAPP GLOBAL STATE
   ============================================================ */

/**
 * There must only ever be ONE active WhatsApp socket.
 */
let sock: ReturnType<typeof makeWASocket> | null = null;

/**
 * Every socket gets a unique generation.
 *
 * If an old socket fires an event after a new socket has been
 * created, that event is ignored.
 */
let whatsappGeneration = 0;

/**
 * True while a socket is being established.
 */
let whatsappConnecting = false;

/**
 * True only after Baileys reports connection === "open".
 */
let whatsappConnectionOpen = false;

/**
 * Only one reconnect timer may exist.
 */
let whatsappReconnectTimer: NodeJS.Timeout | null = null;

let currentWhatsAppQr: string | null = null;

/**
 * Prevent restoring the same trackers multiple times.
 */
let whatsappRestoreInProgress = false;


/* ============================================================
   TRACKER STORAGE
   ============================================================ */

/**
 * Keep references to active trackers.
 *
 * The key is the database contact ID.
 */
const whatsappTrackers =
    new Map<string, WhatsAppTracker>();


/* ============================================================
   BASIC HTTP ROUTES
   ============================================================ */

app.get('/', (_req, res) => {
    res.json({
        ok: true,
        service: 'device-activity-tracker',
        whatsappConnected:
            whatsappConnectionOpen,
    });
});


app.get('/health', (_req, res) => {
    res.json({
        ok: true,
        database: !!pool,
        whatsappConnected:
            whatsappConnectionOpen,
    });
});


/* ============================================================
   DATABASE — MEASUREMENTS
   ============================================================ */

async function saveMeasurement(
    measurement: Measurement,
) {
    if (!pool) {
        return;
    }

    try {
        await pool.query(
            `
            INSERT INTO measurements
            (
                device_id,
                rtt,
                avg,
                median,
                threshold,
                state,
                measured_at
            )
            VALUES
            (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                COALESCE($7::timestamptz, NOW())
            )
            `,
            [
                measurement.deviceId,
                measurement.rtt,
                measurement.avg,
                measurement.median,
                measurement.threshold,
                measurement.state,
                measurement.measuredAt ?? null,
            ],
        );
    } catch (err) {
        console.error(
            '[DB] Failed to save measurement:',
            err,
        );
    }
}


async function getMeasurementHistory(
    deviceId: string,
    limit = 500,
): Promise<Measurement[]> {
    if (!pool) {
        return [];
    }

    try {
        const safeLimit =
            Math.min(
                Math.max(
                    Number(limit) || 500,
                    1,
                ),
                5000,
            );

        const result =
            await pool.query(
                `
                SELECT
                    device_id,
                    rtt,
                    avg,
                    median,
                    threshold,
                    state,
                    measured_at
                FROM measurements
                WHERE device_id = $1
                ORDER BY measured_at DESC
                LIMIT $2
                `,
                [
                    deviceId,
                    safeLimit,
                ],
            );

        return result.rows.map(
            (row) => ({
                deviceId:
                    row.device_id,

                rtt:
                    row.rtt === null
                        ? null
                        : Number(row.rtt),

                avg:
                    row.avg === null
                        ? null
                        : Number(row.avg),

                median:
                    row.median === null
                        ? null
                        : Number(row.median),

                threshold:
                    row.threshold === null
                        ? null
                        : Number(row.threshold),

                state:
                    row.state,

                measuredAt:
                    row.measured_at,
            }),
        );
    } catch (err) {
        console.error(
            '[DB] Failed to get measurement history:',
            err,
        );

        return [];
    }
}


/* ============================================================
   DATABASE — TRACKED CONTACTS
   ============================================================ */

async function saveTrackedContact(
    id: string,
    platform: TrackerPlatform,
    phoneNumber: string,
) {
    if (!pool) {
        return;
    }

    try {
        await pool.query(
            `
            INSERT INTO tracked_contacts
            (
                id,
                platform,
                phone_number
            )
            VALUES
            (
                $1,
                $2,
                $3
            )
            ON CONFLICT (id)
            DO UPDATE SET
                platform =
                    EXCLUDED.platform,
                phone_number =
                    EXCLUDED.phone_number
            `,
            [
                id,
                platform,
                phoneNumber,
            ],
        );

        console.log(
            `[DB] Saved tracked contact: ${platform} ${phoneNumber}`,
        );
    } catch (err) {
        console.error(
            '[DB] Failed to save tracked contact:',
            err,
        );
    }
}


async function removeTrackedContact(
    id: string,
) {
    if (!pool) {
        return;
    }

    try {
        await pool.query(
            `
            DELETE FROM tracked_contacts
            WHERE id = $1
            `,
            [id],
        );

        console.log(
            `[DB] Removed tracked contact: ${id}`,
        );
    } catch (err) {
        console.error(
            '[DB] Failed to remove tracked contact:',
            err,
        );
    }
}


async function getTrackedContacts():
    Promise<TrackedContact[]> {
    if (!pool) {
        return [];
    }

    try {
        const result =
            await pool.query(
                `
                SELECT
                    id,
                    platform,
                    phone_number
                FROM tracked_contacts
                ORDER BY created_at ASC
                `,
            );

        return result.rows.map(
            (row) => ({
                id:
                    row.id,

                platform:
                    row.platform,

                phoneNumber:
                    row.phone_number,
            }),
        );
    } catch (err) {
        console.error(
            '[DB] Failed to get tracked contacts:',
            err,
        );

        return [];
    }
}


/* ============================================================
   MEASUREMENT HANDLER
   ============================================================ */

function handleTrackerUpdate(
    contact: TrackedContact,
    data: any,
) {
    /**
     * The tracker sends:
     *
     * {
     *   devices,
     *   deviceCount,
     *   presence,
     *   median,
     *   threshold
     * }
     *
     * Each device can have its own RTT/state.
     *
     * Persist the primary target/device data.
     */

    const devices =
        Array.isArray(data?.devices)
            ? data.devices
            : [];

    if (devices.length === 0) {
        io.emit(
            'tracker-update',
            {
                deviceId:
                    contact.id,

                ...data,
            },
        );

        return;
    }

    for (const device of devices) {
        const measurement: Measurement = {
            deviceId:
                contact.id,

            rtt:
                typeof device.rtt === 'number'
                    ? device.rtt
                    : null,

            avg:
                typeof device.avg === 'number'
                    ? device.avg
                    : null,

            median:
                typeof data.median === 'number'
                    ? data.median
                    : null,

            threshold:
                typeof data.threshold === 'number'
                    ? data.threshold
                    : null,

            state:
                typeof device.state === 'string'
                    ? device.state
                    : null,

            measuredAt:
                new Date().toISOString(),
        };

        void saveMeasurement(
            measurement,
        );
    }

    /**
     * Send the complete tracker update to the frontend.
     */
    io.emit(
        'tracker-update',
        {
            deviceId:
                contact.id,

            ...data,
        },
    );

    /**
     * Also emit the measurement event used by the existing
     * dashboard if it listens for "measurement".
     */
    if (devices.length > 0) {
        for (const device of devices) {
            io.emit(
                'measurement',
                {
                    deviceId:
                        contact.id,

                    rtt:
                        typeof device.rtt === 'number'
                            ? device.rtt
                            : null,

                    avg:
                        typeof device.avg === 'number'
                            ? device.avg
                            : null,

                    median:
                        typeof data.median === 'number'
                            ? data.median
                            : null,

                    threshold:
                        typeof data.threshold === 'number'
                            ? data.threshold
                            : null,

                    state:
                        device.state ?? null,

                    measuredAt:
                        new Date().toISOString(),
                },
            );
        }
    }
}


/* ============================================================
   CREATE WHATSAPP TRACKER
   ============================================================ */

async function startWhatsAppTracker(
    contact: TrackedContact,
) {
    if (!sock) {
        console.log(
            `[WA] Cannot start tracker ${contact.phoneNumber}: no socket`,
        );

        return;
    }

    if (!whatsappConnectionOpen) {
        console.log(
            `[WA] Cannot start tracker ${contact.phoneNumber}: WhatsApp is not open`,
        );

        return;
    }

    /**
     * Do not create duplicate trackers for the same contact.
     */
    if (
        whatsappTrackers.has(
            contact.id,
        )
    ) {
        console.log(
            `[WA] Tracker already running: ${contact.phoneNumber}`,
        );

        return;
    }

    const tracker =
        new WhatsAppTracker(
            sock,
            contact.phoneNumber,
        );

    tracker.onUpdate =
        (data) => {
            handleTrackerUpdate(
                contact,
                data,
            );
        };

    whatsappTrackers.set(
        contact.id,
        tracker,
    );

    try {
        await tracker.startTracking();

        console.log(
            `[WA] Tracker started: ${contact.phoneNumber}`,
        );
    } catch (err) {
        whatsappTrackers.delete(
            contact.id,
        );

        console.error(
            `[WA] Failed to start tracker ${contact.phoneNumber}:`,
            err,
        );
    }
}


/* ============================================================
   RESTORE WHATSAPP TRACKERS
   ============================================================ */

async function restoreWhatsAppTrackers(
    socket: ReturnType<typeof makeWASocket>,
    generation: number,
) {
    if (whatsappRestoreInProgress) {
        console.log(
            '[RESTORE] WhatsApp restoration already in progress',
        );

        return;
    }

    /**
     * Make absolutely sure this is still the active socket.
     */
    if (
        socket !== sock ||
        generation !== whatsappGeneration
    ) {
        console.log(
            '[RESTORE] Ignoring stale socket restoration',
        );

        return;
    }

    if (!whatsappConnectionOpen) {
        return;
    }

    whatsappRestoreInProgress = true;

    try {
        const contacts =
            await getTrackedContacts();

        const whatsappContacts =
            contacts.filter(
                (contact) =>
                    contact.platform ===
                    'whatsapp',
            );

        console.log(
            `[RESTORE] Found ${whatsappContacts.length} saved WhatsApp tracker(s)`,
        );

        for (
            const contact
            of whatsappContacts
        ) {
            /**
             * Socket may disconnect while restoring contacts.
             */
            if (
                socket !== sock ||
                generation !== whatsappGeneration ||
                !whatsappConnectionOpen
            ) {
                console.log(
                    '[RESTORE] Socket became stale; stopping restoration',
                );

                break;
            }

            console.log(
                `[RESTORE] Restoring WhatsApp tracker: ${contact.phoneNumber}`,
            );

            await startWhatsAppTracker(
                contact,
            );
        }
    } catch (err) {
        console.error(
            '[RESTORE] WhatsApp tracker restoration failed:',
            err,
        );
    } finally {
        whatsappRestoreInProgress = false;
    }
}


/* ============================================================
   SIGNAL RESTORATION
   ============================================================ */

let signalRestoreInProgress =
    false;


async function restoreSignalTrackers() {
    if (signalRestoreInProgress) {
        return;
    }

    signalRestoreInProgress = true;

    try {
        const contacts =
            await getTrackedContacts();

        const signalContacts =
            contacts.filter(
                (contact) =>
                    contact.platform ===
                    'signal',
            );

        console.log(
            `[RESTORE] Found ${signalContacts.length} saved Signal tracker(s)`,
        );

        /**
         * Keep the saved Signal contacts available.
         *
         * The actual Signal tracker implementation can be
         * restored here when its runtime/API is available.
         */
        for (
            const contact
            of signalContacts
        ) {
            console.log(
                `[RESTORE] Signal tracker saved: ${contact.phoneNumber}`,
            );
        }
    } catch (err) {
        console.error(
            '[RESTORE] Signal restoration failed:',
            err,
        );
    } finally {
        signalRestoreInProgress = false;
    }
}


/* ============================================================
   WHATSAPP RECONNECT CONTROL
   ============================================================ */

function clearWhatsAppReconnectTimer() {
    if (
        whatsappReconnectTimer
    ) {
        clearTimeout(
            whatsappReconnectTimer,
        );

        whatsappReconnectTimer =
            null;
    }
}


function scheduleWhatsAppReconnect() {
    /**
     * Never create multiple reconnect timers.
     */
    if (
        whatsappReconnectTimer
    ) {
        console.log(
            '[WA] Reconnect already scheduled',
        );

        return;
    }

    console.log(
        '[WA] Scheduling WhatsApp reconnect in 2 seconds',
    );

    whatsappReconnectTimer =
        setTimeout(
            () => {
                whatsappReconnectTimer =
                    null;

                void connectToWhatsApp();
            },
            2000,
        );
}


/**
 * Completely invalidate the current socket.
 *
 * This is the key part of the duplicate-connection fix.
 */
function invalidateWhatsAppSocket(
    socketToInvalidate?:
        ReturnType<typeof makeWASocket>,
) {
    /**
     * If a stale socket tries to invalidate the current socket,
     * ignore it.
     */
    if (
        socketToInvalidate &&
        sock &&
        socketToInvalidate !== sock
    ) {
        return;
    }

    /**
     * Increment generation FIRST.
     *
     * Any events emitted after this point become stale.
     */
    whatsappGeneration++;

    sock = null;

    whatsappConnectionOpen =
        false;

    currentWhatsAppQr =
        null;

    io.emit(
        'connection-closed',
    );
}


/* ============================================================
   WHATSAPP CONNECTION
   ============================================================ */

async function connectToWhatsApp() {
    if (!pool) {
        console.error(
            '[WA] DATABASE_URL is required for WhatsApp authentication persistence',
        );

        return;
    }

    /**
     * HARD LOCK:
     *
     * Don't create another socket while one is being created.
     */
    if (whatsappConnecting) {
        console.log(
            '[WA] Connection attempt already in progress — skipping',
        );

        return;
    }

    /**
     * HARD LOCK:
     *
     * Don't create another socket while one is already open.
     */
    if (
        sock &&
        whatsappConnectionOpen
    ) {
        console.log(
            '[WA] WhatsApp already connected — skipping duplicate connection',
        );

        return;
    }

    /**
     * Also don't create another socket if one exists and is still
     * connecting.
     */
    if (
        sock &&
        !whatsappConnectionOpen
    ) {
        console.log(
            '[WA] Existing WhatsApp socket is still connecting — skipping',
        );

        return;
    }

    whatsappConnecting =
        true;

    /**
     * Each socket receives its own generation.
     */
    const generation =
        ++whatsappGeneration;

    console.log(
        `[WA] Starting WhatsApp connection generation ${generation}`,
    );

    try {
        clearWhatsAppReconnectTimer();

        /**
         * Load persistent credentials from PostgreSQL.
         */
        const {
            state,
            saveCreds,
        } =
            await createDatabaseAuthState(
                pool,
                'whatsapp-main',
            );

        /**
         * Make sure another connection wasn't created while
         * PostgreSQL was loading.
         */
        if (
            generation !==
            whatsappGeneration
        ) {
            console.log(
                `[WA] Generation ${generation} became stale before socket creation`,
            );

            whatsappConnecting =
                false;

            return;
        }

        if (sock) {
            console.log(
                '[WA] A socket appeared during auth loading — refusing duplicate socket',
            );

            whatsappConnecting =
                false;

            return;
        }

        /**
         * Create exactly ONE Baileys socket.
         */
        const newSock =
            makeWASocket({
                auth: state,

                logger:
                    pino({
                        level: 'silent',
                    }),

                markOnlineOnConnect:
                    true,

                printQRInTerminal:
                    false,
            });

        /**
         * Assign it immediately.
         */
        sock =
            newSock;

        console.log(
            `[WA] Socket created — generation ${generation}`,
        );

        /**
         * Save every credential change to PostgreSQL.
         */
        newSock.ev.on(
            'creds.update',
            async () => {
                try {
                    await saveCreds();
                } catch (err) {
                    console.error(
                        '[WA] Failed to save credentials:',
                        err,
                    );
                }
            },
        );

        /* ========================================================
           CONNECTION UPDATE
        ======================================================== */

        newSock.ev.on(
            'connection.update',
            async (
                update: any,
            ) => {
                const {
                    connection,
                    lastDisconnect,
                    qr,
                } = update;

                /**
                 * CRITICAL GENERATION CHECK.
                 *
                 * If this isn't the current socket, ignore it.
                 *
                 * This prevents:
                 *
                 * OLD SOCKET
                 *      ↓
                 * connection.close
                 *      ↓
                 * reconnect
                 *      ↓
                 * NEW SOCKET
                 *
                 * from being triggered repeatedly.
                 */
                if (
                    newSock !== sock ||
                    generation !==
                        whatsappGeneration
                ) {
                    console.log(
                        `[WA] Ignoring event from stale generation ${generation}`,
                    );

                    return;
                }

                /* ------------------------------------------------
                   QR
                ------------------------------------------------ */

                if (qr) {
                    currentWhatsAppQr =
                        qr;

                    io.emit(
                        'qr',
                        qr,
                    );

                    console.log(
                        '[WA] QR code received',
                    );
                }

                /* ------------------------------------------------
                   CONNECTING
                ------------------------------------------------ */

                if (
                    connection ===
                    'connecting'
                ) {
                    console.log(
                        `[WA] Connecting... generation ${generation}`,
                    );

                    whatsappConnectionOpen =
                        false;

                    io.emit(
                        'connection-state',
                        'connecting',
                    );
                }

                /* ------------------------------------------------
                   OPEN
                ------------------------------------------------ */

                if (
                    connection ===
                    'open'
                ) {
                    /**
                     * Make sure this socket is STILL the current
                     * socket before changing global state.
                     */
                    if (
                        newSock !== sock ||
                        generation !==
                            whatsappGeneration
                    ) {
                        return;
                    }

                    console.log(
                        `[WA] WhatsApp OPEN — generation ${generation}`,
                    );

                    whatsappConnectionOpen =
                        true;

                    whatsappConnecting =
                        false;

                    currentWhatsAppQr =
                        null;

                    io.emit(
                        'connection-open',
                    );

                    io.emit(
                        'connection-state',
                        'open',
                    );

                    /**
                     * Restore the persistent trackers only after
                     * the socket is actually open.
                     */
                    await restoreWhatsAppTrackers(
                        newSock,
                        generation,
                    );
                }

                /* ------------------------------------------------
                   CLOSE
                ------------------------------------------------ */

                if (
                    connection ===
                    'close'
                ) {
                    /**
                     * Ignore close events from stale sockets.
                     */
                    if (
                        newSock !== sock ||
                        generation !==
                            whatsappGeneration
                    ) {
                        console.log(
                            `[WA] Ignoring close from stale generation ${generation}`,
                        );

                        return;
                    }

                    const statusCode =
                        (
                            lastDisconnect
                                ?.error as Boom
                        )
                            ?.output
                            ?.statusCode;

                    const loggedOut =
                        statusCode ===
                        DisconnectReason.loggedOut;

                    const shouldReconnect =
                        !loggedOut;

                    console.log(
                        `[WA] Connection closed — statusCode=${statusCode}, reconnect=${shouldReconnect}, generation=${generation}`,
                    );

                    /**
                     * STOP all trackers belonging to this socket.
                     */
                    for (
                        const tracker
                        of whatsappTrackers.values()
                    ) {
                        try {
                            tracker.stopTracking();
                        } catch {
                            // Ignore cleanup errors.
                        }
                    }

                    whatsappTrackers.clear();

                    /**
                     * IMPORTANT:
                     *
                     * Invalidate BEFORE scheduling reconnect.
                     *
                     * This makes every subsequent event from this
                     * socket stale.
                     */
                    invalidateWhatsAppSocket(
                        newSock,
                    );

                    whatsappConnecting =
                        false;

                    if (loggedOut) {
                        console.log(
                            '[WA] WhatsApp logged out. Automatic reconnect disabled.',
                        );

                        io.emit(
                            'connection-state',
                            'logged-out',
                        );

                        return;
                    }

                    io.emit(
                        'connection-state',
                        'reconnecting',
                    );

                    if (
                        shouldReconnect
                    ) {
                        scheduleWhatsAppReconnect();
                    }
                }
            },
        );
    } catch (err) {
        console.error(
            '[WA] Failed to create WhatsApp connection:',
            err,
        );

        /**
         * Only invalidate if this is still the active generation.
         */
        if (
            generation ===
            whatsappGeneration
        ) {
            invalidateWhatsAppSocket();
        }

        whatsappConnecting =
            false;

        scheduleWhatsAppReconnect();
    }
}


/* ============================================================
   SOCKET.IO
   ============================================================ */

io.on(
    'connection',
    (client) => {
        console.log(
            `[SOCKET] Client connected: ${client.id}`,
        );

        /**
         * Immediately tell the frontend the current state.
         */
        client.emit(
            'connection-state',
            whatsappConnectionOpen
                ? 'open'
                : 'closed',
        );

        /**
         * Send current QR if one exists.
         */
        if (
            currentWhatsAppQr
        ) {
            client.emit(
                'qr',
                currentWhatsAppQr,
            );
        }


        /* ========================================================
           GET TRACKED CONTACTS
        ======================================================== */

        client.on(
            'get-tracked-contacts',
            async () => {
                try {
                    const contacts =
                        await getTrackedContacts();

                    client.emit(
                        'tracked-contacts',
                        contacts,
                    );
                } catch (err) {
                    console.error(
                        '[SOCKET] Failed to get tracked contacts:',
                        err,
                    );

                    client.emit(
                        'tracked-contacts',
                        [],
                    );
                }
            },
        );


        /* ========================================================
           ADD CONTACT
        ======================================================== */

        client.on(
            'add-contact',
            async (
                data: any,
            ) => {
                try {
                    const platform =
                        data?.platform as TrackerPlatform;

                    const phoneNumber =
                        String(
                            data?.phoneNumber ??
                                '',
                        ).trim();

                    const id =
                        String(
                            data?.id ??
                                `${platform}:${phoneNumber}`,
                        ).trim();

                    if (
                        platform !==
                            'whatsapp' &&
                        platform !==
                            'signal'
                    ) {
                        client.emit(
                            'error-message',
                            'Invalid platform',
                        );

                        return;
                    }

                    if (
                        !phoneNumber
                    ) {
                        client.emit(
                            'error-message',
                            'Phone number is required',
                        );

                        return;
                    }

                    /**
                     * Persist FIRST.
                     */
                    await saveTrackedContact(
                        id,
                        platform,
                        phoneNumber,
                    );

                    /**
                     * Send updated contact list.
                     */
                    const contacts =
                        await getTrackedContacts();

                    io.emit(
                        'tracked-contacts',
                        contacts,
                    );

                    /**
                     * If WhatsApp is already connected,
                     * start immediately.
                     */
                    if (
                        platform ===
                            'whatsapp' &&
                        sock &&
                        whatsappConnectionOpen
                    ) {
                        const savedContact:
                            TrackedContact =
                            {
                                id,
                                platform,
                                phoneNumber,
                            };

                        await startWhatsAppTracker(
                            savedContact,
                        );
                    }
                } catch (err) {
                    console.error(
                        '[SOCKET] add-contact error:',
                        err,
                    );
                }
            },
        );


        /* ========================================================
           REMOVE CONTACT
        ======================================================== */

        client.on(
            'remove-contact',
            async (
                data: any,
            ) => {
                try {
                    const id =
                        String(
                            data?.id ??
                                '',
                        ).trim();

                    if (!id) {
                        return;
                    }

                    /**
                     * Stop active tracker first.
                     */
                    const tracker =
                        whatsappTrackers.get(
                            id,
                        );

                    if (tracker) {
                        try {
                            tracker.stopTracking();
                        } catch {
                            // Ignore.
                        }

                        whatsappTrackers.delete(
                            id,
                        );
                    }

                    /**
                     * Delete persistent contact.
                     */
                    await removeTrackedContact(
                        id,
                    );

                    const contacts =
                        await getTrackedContacts();

                    io.emit(
                        'tracked-contacts',
                        contacts,
                    );
                } catch (err) {
                    console.error(
                        '[SOCKET] remove-contact error:',
                        err,
                    );
                }
            },
        );


        /* ========================================================
           GET MEASUREMENT HISTORY
        ======================================================== */

        client.on(
            'get-measurement-history',
            async (
                data: any,
            ) => {
                try {
                    const deviceId =
                        String(
                            data?.deviceId ??
                                '',
                        ).trim();

                    const limit =
                        Number(
                            data?.limit ??
                                500,
                        );

                    if (!deviceId) {
                        return;
                    }

                    const history =
                        await getMeasurementHistory(
                            deviceId,
                            limit,
                        );

                    client.emit(
                        'measurement-history',
                        {
                            deviceId,
                            history,
                        },
                    );
                } catch (err) {
                    console.error(
                        '[SOCKET] Failed to get measurement history:',
                        err,
                    );
                }
            },
        );


        /* ========================================================
           DISCONNECT
        ======================================================== */

        client.on(
            'disconnect',
            () => {
                console.log(
                    `[SOCKET] Client disconnected: ${client.id}`,
                );
            },
        );
    },
);


/* ============================================================
   SERVER START
   ============================================================ */

server.listen(
    PORT,
    '0.0.0.0',
    async () => {
        console.log(
            `[SERVER] Listening on port ${PORT}`,
        );

        /**
         * Check PostgreSQL.
         */
        if (pool) {
            try {
                await pool.query(
                    'SELECT 1',
                );

                console.log(
                    '[DB] PostgreSQL connection OK',
                );
            } catch (err) {
                console.error(
                    '[DB] PostgreSQL connection failed:',
                    err,
                );
            }
        }

        /**
         * Restore saved Signal contacts.
         */
        void restoreSignalTrackers();

        /**
         * Start exactly ONE WhatsApp connection.
         */
        void connectToWhatsApp();
    },
);


/* ============================================================
   SHUTDOWN
   ============================================================ */

async function shutdown(
    signal: string,
) {
    console.log(
        `[SERVER] Received ${signal}, shutting down...`,
    );

    /**
     * Never reconnect while shutting down.
     */
    clearWhatsAppReconnectTimer();

    /**
     * Invalidate every existing WhatsApp event listener.
     */
    whatsappGeneration++;

    whatsappConnecting =
        false;

    whatsappConnectionOpen =
        false;

    /**
     * Stop trackers.
     */
    for (
        const tracker
        of whatsappTrackers.values()
    ) {
        try {
            tracker.stopTracking();
        } catch {
            // Ignore.
        }
    }

    whatsappTrackers.clear();

    /**
     * Detach global socket.
     */
    const currentSocket =
        sock;

    sock = null;

    currentWhatsAppQr =
        null;

    /**
     * Close Baileys socket.
     */
    if (currentSocket) {
        try {
            currentSocket.end(
                undefined,
            );
        } catch {
            // Ignore.
        }
    }

    /**
     * Close database.
     */
    if (pool) {
        try {
            await pool.end();
        } catch (err) {
            console.error(
                '[DB] Error closing PostgreSQL:',
                err,
            );
        }
    }

    /**
     * Close HTTP server.
     */
    server.close(
        () => {
            process.exit(0);
        },
    );

    /**
     * Don't hang forever.
     */
    setTimeout(
        () => {
            process.exit(0);
        },
        5000,
    );
}


process.on(
    'SIGTERM',
    () => {
        void shutdown(
            'SIGTERM',
        );
    },
);

process.on(
    'SIGINT',
    () => {
        void shutdown(
            'SIGINT',
        );
    },
);