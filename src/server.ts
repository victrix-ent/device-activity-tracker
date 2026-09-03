import 'dotenv/config';
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
    createWhatsAppTracker,
} from './tracker.js';

const PORT = Number(process.env.PORT || 3000);

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
    },
});

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
    console.warn('[DB] DATABASE_URL is not configured');
} else {
    pool.on('error', (err) => {
        console.error('[DB] Unexpected PostgreSQL pool error:', err);
    });
}

/* ============================================================
   TYPES
   ============================================================ */

type TrackerPlatform = 'whatsapp' | 'signal';

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
   WHATSAPP STATE
   ============================================================ */

/**
 * IMPORTANT:
 *
 * Only this socket is allowed to be the active WhatsApp socket.
 *
 * generation is incremented every time the active socket is
 * invalidated. Events coming from an older socket are ignored.
 */
let sock: ReturnType<typeof makeWASocket> | null = null;

let whatsappGeneration = 0;

let whatsappConnecting = false;

let whatsappConnectionOpen = false;

let whatsappReconnectTimer: NodeJS.Timeout | null = null;

let currentWhatsAppQr: string | null = null;

let whatsappRestoreInProgress = false;

/* ============================================================
   SIGNAL STATE
   ============================================================ */

let signalRestoreInProgress = false;

/* ============================================================
   BASIC ROUTES
   ============================================================ */

app.get('/', (_req, res) => {
    res.json({
        ok: true,
        service: 'device-activity-tracker',
        whatsappConnected: whatsappConnectionOpen,
    });
});

app.get('/health', (_req, res) => {
    res.json({
        ok: true,
        whatsappConnected: whatsappConnectionOpen,
        database: !!pool,
    });
});

/* ============================================================
   DATABASE HELPERS
   ============================================================ */

async function saveMeasurement(measurement: Measurement) {
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
                ($1, $2, $3, $4, $5, $6, COALESCE($7::timestamptz, NOW()))
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
        console.error('[DB] Failed to save measurement:', err);
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
        const result = await pool.query(
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
            [deviceId, Math.min(Math.max(limit, 1), 5000)],
        );

        return result.rows.map((row) => ({
            deviceId: row.device_id,
            rtt: row.rtt === null ? null : Number(row.rtt),
            avg: row.avg === null ? null : Number(row.avg),
            median: row.median === null ? null : Number(row.median),
            threshold:
                row.threshold === null
                    ? null
                    : Number(row.threshold),
            state: row.state,
            measuredAt: row.measured_at,
        }));
    } catch (err) {
        console.error(
            '[DB] Failed to get measurement history:',
            err,
        );

        return [];
    }
}

/* ============================================================
   TRACKED CONTACT DATABASE
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
                ($1, $2, $3)
            ON CONFLICT (id)
            DO UPDATE SET
                platform = EXCLUDED.platform,
                phone_number = EXCLUDED.phone_number
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

async function removeTrackedContact(id: string) {
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

        console.log(`[DB] Removed tracked contact: ${id}`);
    } catch (err) {
        console.error(
            '[DB] Failed to remove tracked contact:',
            err,
        );
    }
}

async function getTrackedContacts(): Promise<TrackedContact[]> {
    if (!pool) {
        return [];
    }

    try {
        const result = await pool.query(
            `
            SELECT
                id,
                platform,
                phone_number
            FROM tracked_contacts
            ORDER BY created_at ASC
            `,
        );

        return result.rows.map((row) => ({
            id: row.id,
            platform: row.platform,
            phoneNumber: row.phone_number,
        }));
    } catch (err) {
        console.error(
            '[DB] Failed to get tracked contacts:',
            err,
        );

        return [];
    }
}

/* ============================================================
   WHATSAPP TRACKER RESTORATION
   ============================================================ */

async function restoreWhatsAppTrackers(
    socket: ReturnType<typeof makeWASocket>,
) {
    if (whatsappRestoreInProgress) {
        console.log(
            '[RESTORE] WhatsApp tracker restoration already running',
        );

        return;
    }

    if (socket !== sock) {
        console.log(
            '[RESTORE] Ignoring restoration for stale WhatsApp socket',
        );

        return;
    }

    whatsappRestoreInProgress = true;

    try {
        const contacts = await getTrackedContacts();

        const whatsappContacts = contacts.filter(
            (contact) =>
                contact.platform === 'whatsapp',
        );

        console.log(
            `[RESTORE] Found ${whatsappContacts.length} saved WhatsApp tracker(s)`,
        );

        for (const contact of whatsappContacts) {
            /**
             * Check again before creating each tracker.
             *
             * The socket could have disconnected while we were
             * restoring another tracker.
             */
            if (socket !== sock || !whatsappConnectionOpen) {
                console.log(
                    '[RESTORE] WhatsApp socket is no longer active; stopping restoration',
                );

                break;
            }

            try {
                console.log(
                    `[RESTORE] Restoring WhatsApp tracker: ${contact.phoneNumber}`,
                );

                const tracker = createWhatsAppTracker(
                    socket,
                    contact.phoneNumber,
                );

                /**
                 * Depending on your tracker implementation,
                 * createWhatsAppTracker may return an object with
                 * start()/startTracking().
                 *
                 * Try the available method without requiring a
                 * specific implementation.
                 */
                if (
                    tracker &&
                    typeof (tracker as any).start === 'function'
                ) {
                    await (tracker as any).start();
                } else if (
                    tracker &&
                    typeof (tracker as any).startTracking === 'function'
                ) {
                    await (tracker as any).startTracking();
                }

                console.log(
                    `[RESTORE] Restored WhatsApp tracker: ${contact.phoneNumber}`,
                );
            } catch (err) {
                console.error(
                    `[RESTORE] Failed to restore WhatsApp tracker ${contact.phoneNumber}:`,
                    err,
                );
            }
        }
    } catch (err) {
        console.error(
            '[RESTORE] Failed to restore WhatsApp trackers:',
            err,
        );
    } finally {
        whatsappRestoreInProgress = false;
    }
}

/* ============================================================
   SIGNAL TRACKER RESTORATION
   ============================================================ */

async function restoreSignalTrackers() {
    if (signalRestoreInProgress) {
        return;
    }

    signalRestoreInProgress = true;

    try {
        const contacts = await getTrackedContacts();

        const signalContacts = contacts.filter(
            (contact) =>
                contact.platform === 'signal',
        );

        console.log(
            `[RESTORE] Found ${signalContacts.length} saved Signal tracker(s)`,
        );

        /**
         * Keep the Signal restoration hook here.
         *
         * Your existing Signal tracker implementation can be
         * inserted here if it exposes a restore/start function.
         */
        for (const contact of signalContacts) {
            console.log(
                `[RESTORE] Signal tracker saved: ${contact.phoneNumber}`,
            );
        }
    } catch (err) {
        console.error(
            '[RESTORE] Failed to restore Signal trackers:',
            err,
        );
    } finally {
        signalRestoreInProgress = false;
    }
}

/* ============================================================
   WHATSAPP RECONNECT MANAGEMENT
   ============================================================ */

/**
 * Cancel the currently scheduled reconnect.
 */
function clearWhatsAppReconnectTimer() {
    if (whatsappReconnectTimer) {
        clearTimeout(whatsappReconnectTimer);
        whatsappReconnectTimer = null;
    }
}

/**
 * Schedule exactly ONE reconnect.
 *
 * This is deliberately centralized so multiple close events
 * cannot create multiple sockets.
 */
function scheduleWhatsAppReconnect() {
    if (whatsappReconnectTimer) {
        console.log(
            '[WA] Reconnect already scheduled, skipping duplicate',
        );

        return;
    }

    console.log(
        '[WA] Scheduling reconnect in 2 seconds',
    );

    whatsappReconnectTimer = setTimeout(() => {
        whatsappReconnectTimer = null;

        void connectToWhatsApp();
    }, 2000);
}

/**
 * Invalidate the current socket.
 *
 * Incrementing generation means all listeners belonging to the
 * old socket become stale immediately.
 */
function invalidateWhatsAppSocket(
    socketToInvalidate?: ReturnType<typeof makeWASocket>,
) {
    if (
        socketToInvalidate &&
        sock &&
        socketToInvalidate !== sock
    ) {
        return;
    }

    whatsappGeneration++;

    sock = null;

    whatsappConnectionOpen = false;

    currentWhatsAppQr = null;

    io.emit('connection-closed');
}

/* ============================================================
   WHATSAPP CONNECTION
   ============================================================ */

async function connectToWhatsApp() {
    if (!pool) {
        console.error(
            '[WA] DATABASE_URL is required for WhatsApp session persistence',
        );

        return;
    }

    /**
     * If we already have an active socket or a connection attempt,
     * NEVER create another one.
     */
    if (whatsappConnecting) {
        console.log(
            '[WA] Connection attempt already in progress, skipping',
        );

        return;
    }

    if (sock && whatsappConnectionOpen) {
        console.log(
            '[WA] WhatsApp is already connected, skipping duplicate connection',
        );

        return;
    }

    if (sock && !whatsappConnectionOpen) {
        console.log(
            '[WA] Existing WhatsApp socket is still being established; skipping duplicate connection',
        );

        return;
    }

    whatsappConnecting = true;

    const generation = ++whatsappGeneration;

    console.log(
        `[WA] Starting WhatsApp connection generation ${generation}`,
    );

    try {
        clearWhatsAppReconnectTimer();

        const {
            state,
            saveCreds,
        } = await createDatabaseAuthState(
            pool,
            'whatsapp-main',
        );

        /**
         * Another connection could theoretically have started while
         * the database auth state was loading.
         */
        if (generation !== whatsappGeneration) {
            console.log(
                `[WA] Generation ${generation} became stale before socket creation`,
            );

            return;
        }

        if (sock) {
            console.log(
                '[WA] Socket appeared while loading auth state; refusing to create another socket',
            );

            return;
        }

        const newSock = makeWASocket({
            auth: state,

            logger: pino({
                level: 'debug',
            }),

            markOnlineOnConnect: true,

            printQRInTerminal: false,

            /**
             * Keep the normal Baileys defaults.
             */
        });

        /**
         * IMPORTANT:
         *
         * Assign the socket exactly once.
         */
        sock = newSock;

        console.log(
            `[WA] Socket created for generation ${generation}`,
        );

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

        newSock.ev.on(
            'connection.update',
            async (update: any) => {
                const {
                    connection,
                    lastDisconnect,
                    qr,
                } = update;

                /**
                 * CRITICAL:
                 *
                 * Ignore EVERY event emitted by an old socket.
                 *
                 * This is what prevents stale sockets from spawning
                 * another connection.
                 */
                if (
                    newSock !== sock ||
                    generation !== whatsappGeneration
                ) {
                    console.log(
                        `[WA] Ignoring stale socket event from generation ${generation}`,
                    );

                    return;
                }

                if (qr) {
                    currentWhatsAppQr = qr;

                    io.emit(
                        'qr',
                        qr,
                    );

                    console.log(
                        '[WA] QR code received',
                    );
                }

                if (connection === 'connecting') {
                    console.log(
                        `[WA] Connecting... generation ${generation}`,
                    );

                    whatsappConnectionOpen = false;

                    io.emit(
                        'connection-state',
                        'connecting',
                    );
                }

                if (connection === 'open') {
                    console.log(
                        `[WA] WhatsApp connection OPEN generation ${generation}`,
                    );

                    whatsappConnectionOpen = true;

                    currentWhatsAppQr = null;

                    whatsappConnecting = false;

                    io.emit(
                        'connection-open',
                    );

                    io.emit(
                        'connection-state',
                        'open',
                    );

                    /**
                     * Restore only after this socket is confirmed open.
                     */
                    await restoreWhatsAppTrackers(
                        newSock,
                    );
                }

                if (connection === 'close') {
                    const statusCode =
                        (lastDisconnect?.error as Boom)
                            ?.output
                            ?.statusCode;

                    const loggedOut =
                        statusCode === DisconnectReason.loggedOut;

                    const shouldReconnect =
                        !loggedOut;

                    console.log(
                        `[WA] Connection closed. statusCode=${statusCode}, reconnect=${shouldReconnect}, generation=${generation}`,
                    );

                    /**
                     * Mark this generation dead BEFORE doing anything
                     * that could schedule another connection.
                     */
                    if (
                        newSock === sock &&
                        generation === whatsappGeneration
                    ) {
                        invalidateWhatsAppSocket(
                            newSock,
                        );
                    }

                    whatsappConnecting = false;

                    if (loggedOut) {
                        console.log(
                            '[WA] Logged out. Automatic reconnect disabled.',
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

                    /**
                     * Exactly one reconnect timer.
                     */
                    if (shouldReconnect) {
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
        if (generation === whatsappGeneration) {
            invalidateWhatsAppSocket();
        }

        whatsappConnecting = false;

        scheduleWhatsAppReconnect();
    } finally {
        /**
         * Do NOT set whatsappConnecting=false here if the socket
         * is still connecting.
         *
         * connection.update('open'/'close') handles that.
         *
         * This is intentional.
         */
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
         * Send current WhatsApp state immediately.
         */
        client.emit(
            'connection-state',
            whatsappConnectionOpen
                ? 'open'
                : 'closed',
        );

        if (currentWhatsAppQr) {
            client.emit(
                'qr',
                currentWhatsAppQr,
            );
        }

        /* --------------------------------------------------------
           GET TRACKED CONTACTS
        -------------------------------------------------------- */

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
                        '[SOCKET] Failed to send tracked contacts:',
                        err,
                    );

                    client.emit(
                        'tracked-contacts',
                        [],
                    );
                }
            },
        );

        /* --------------------------------------------------------
           ADD CONTACT
        -------------------------------------------------------- */

        client.on(
            'add-contact',
            async (data: any) => {
                try {
                    const platform =
                        data?.platform as TrackerPlatform;

                    const phoneNumber =
                        String(
                            data?.phoneNumber ?? '',
                        ).trim();

                    const id =
                        String(
                            data?.id ??
                            `${platform}:${phoneNumber}`,
                        );

                    if (
                        platform !== 'whatsapp' &&
                        platform !== 'signal'
                    ) {
                        client.emit(
                            'error-message',
                            'Invalid platform',
                        );

                        return;
                    }

                    if (!phoneNumber) {
                        client.emit(
                            'error-message',
                            'Phone number is required',
                        );

                        return;
                    }

                    await saveTrackedContact(
                        id,
                        platform,
                        phoneNumber,
                    );

                    const contacts =
                        await getTrackedContacts();

                    io.emit(
                        'tracked-contacts',
                        contacts,
                    );

                    /**
                     * If WhatsApp is currently connected,
                     * start the tracker immediately.
                     */
                    if (
                        platform === 'whatsapp' &&
                        sock &&
                        whatsappConnectionOpen
                    ) {
                        try {
                            const tracker =
                                createWhatsAppTracker(
                                    sock,
                                    phoneNumber,
                                );

                            if (
                                tracker &&
                                typeof (tracker as any).start ===
                                    'function'
                            ) {
                                await (tracker as any).start();
                            } else if (
                                tracker &&
                                typeof (tracker as any)
                                    .startTracking ===
                                    'function'
                            ) {
                                await (tracker as any)
                                    .startTracking();
                            }

                            console.log(
                                `[WA] Started tracker for ${phoneNumber}`,
                            );
                        } catch (err) {
                            console.error(
                                `[WA] Failed to start tracker for ${phoneNumber}:`,
                                err,
                            );
                        }
                    }
                } catch (err) {
                    console.error(
                        '[SOCKET] add-contact error:',
                        err,
                    );
                }
            },
        );

        /* --------------------------------------------------------
           REMOVE CONTACT
        -------------------------------------------------------- */

        client.on(
            'remove-contact',
            async (data: any) => {
                try {
                    const id =
                        String(
                            data?.id ?? '',
                        ).trim();

                    if (!id) {
                        return;
                    }

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

        /* --------------------------------------------------------
           GET MEASUREMENT HISTORY
        -------------------------------------------------------- */

        client.on(
            'get-measurement-history',
            async (data: any) => {
                try {
                    const deviceId =
                        String(
                            data?.deviceId ?? '',
                        ).trim();

                    const limit =
                        Number(
                            data?.limit ?? 500,
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

        /* --------------------------------------------------------
           DISCONNECT
        -------------------------------------------------------- */

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
   TRACKER MEASUREMENT EVENT HANDLER
   ============================================================ */

/**
 * This function is intentionally generic so your existing tracker
 * can call it with the measurement object.
 */
function onTrackerMeasurement(
    measurement: Measurement,
) {
    /**
     * Broadcast immediately.
     */
    io.emit(
        'measurement',
        measurement,
    );

    /**
     * Persist permanently.
     */
    void saveMeasurement(
        measurement,
    );
}

/* ============================================================
   OPTIONAL GLOBAL TRACKER HOOK
   ============================================================ */

/**
 * Make the measurement handler available globally so tracker
 * implementations that expect a global callback can use it.
 */
(globalThis as any).onTrackerMeasurement =
    onTrackerMeasurement;

/* ============================================================
   START SERVER
   ============================================================ */

server.listen(
    PORT,
    '0.0.0.0',
    async () => {
        console.log(
            `[SERVER] Listening on port ${PORT}`,
        );

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
         * Restore Signal contacts independently.
         */
        void restoreSignalTrackers();

        /**
         * Start exactly one WhatsApp connection.
         */
        void connectToWhatsApp();
    },
);

/* ============================================================
   PROCESS SHUTDOWN
   ============================================================ */

async function shutdown(
    signal: string,
) {
    console.log(
        `[SERVER] Received ${signal}, shutting down...`,
    );

    clearWhatsAppReconnectTimer();

    /**
     * Invalidate the socket so no stale connection.update event
     * can schedule another connection during shutdown.
     */
    whatsappGeneration++;

    whatsappConnecting = false;

    whatsappConnectionOpen = false;

    const currentSocket = sock;

    sock = null;

    try {
        if (currentSocket) {
            try {
                currentSocket.end(
                    undefined,
                );
            } catch {
                // Ignore socket shutdown errors.
            }
        }
    } catch {
        // Ignore.
    }

    try {
        await pool?.end();
    } catch (err) {
        console.error(
            '[DB] Error closing PostgreSQL pool:',
            err,
        );
    }

    server.close(
        () => {
            process.exit(0);
        },
    );

    setTimeout(
        () => process.exit(0),
        5000,
    );
}

process.on(
    'SIGTERM',
    () => {
        void shutdown('SIGTERM');
    },
);

process.on(
    'SIGINT',
    () => {
        void shutdown('SIGINT');
    },
);