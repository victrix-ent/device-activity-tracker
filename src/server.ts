/**
 * Device Activity Tracker - Web Server
 *
 * HTTP server with Socket.IO for real-time tracking visualization.
 * Provides REST API and WebSocket interface for the React frontend.
 *
 * For educational and research purposes only.
 */

import express from 'express';
import basicAuth from 'express-basic-auth';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import path from 'path';
import makeWASocket, {
    DisconnectReason
} from '@whiskeysockets/baileys';

import {
    createDatabaseAuthState
} from './baileys-db-auth.js';
import { pino } from 'pino';
import { Boom } from '@hapi/boom';
import { Pool } from 'pg';
import {
    WhatsAppTracker,
    ProbeMethod
} from './tracker.js';
import {
    SignalTracker,
    getSignalAccounts,
    checkSignalNumber
} from './signal-tracker.js';

// Configuration
const SIGNAL_API_URL =
    process.env.SIGNAL_API_URL ||
    'http://localhost:8080';

const app = express();
app.use(cors());

// PostgreSQL
const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
    console.warn(
        '[DB] DATABASE_URL is not configured. Database persistence is disabled.'
    );
}

const pool = databaseUrl
    ? new Pool({
        connectionString: databaseUrl,
        ssl: {
            rejectUnauthorized: false
        },
    })
    : null;

if (pool) {
    pool.on('error', (err) => {
        console.error(
            '[DB] Unexpected PostgreSQL pool error:',
            err
        );
    });
}


// ============================================================
// MEASUREMENT PERSISTENCE
// ============================================================

// Save a tracker measurement to PostgreSQL
async function saveMeasurement(
    jid: string,
    updateData: any
) {
    if (!pool) return;

    try {
        const device =
            updateData.devices?.[0];

        await pool.query(
            `
            INSERT INTO measurements (
                device_id,
                rtt,
                avg,
                median,
                threshold,
                state,
                measured_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            `,
            [
                jid,
                device?.rtt ?? null,
                device?.avg ?? null,
                updateData.median ?? null,
                updateData.threshold ?? null,
                device?.state ?? null,
            ]
        );
    } catch (err) {
        console.error(
            '[DB] Failed to save measurement:',
            err
        );
    }
}


// Load historical measurements
async function getMeasurementHistory(
    jid: string,
    limit = 500
) {
    if (!pool) return [];

    try {
        const result = await pool.query(
            `
            SELECT
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
            [jid, limit]
        );

        return result.rows.reverse();
    } catch (err) {
        console.error(
            '[DB] Failed to load measurement history:',
            err
        );

        return [];
    }
}


// ============================================================
// TRACKED CONTACT PERSISTENCE
// ============================================================

// Save a tracked contact
async function saveTrackedContact(
    id: string,
    platform: Platform,
    phoneNumber: string
) {
    if (!pool) return;

    try {
        await pool.query(
            `
            INSERT INTO tracked_contacts (
                id,
                platform,
                phone_number
            )
            VALUES ($1, $2, $3)
            ON CONFLICT (id)
            DO UPDATE SET
                platform = EXCLUDED.platform,
                phone_number = EXCLUDED.phone_number
            `,
            [
                id,
                platform,
                phoneNumber
            ]
        );

        console.log(
            `[DB] Saved tracked contact: ${id}`
        );
    } catch (err) {
        console.error(
            '[DB] Failed to save tracked contact:',
            err
        );
    }
}


// Remove a tracked contact
async function removeTrackedContact(
    id: string
) {
    if (!pool) return;

    try {
        await pool.query(
            `
            DELETE FROM tracked_contacts
            WHERE id = $1
            `,
            [id]
        );

        console.log(
            `[DB] Removed tracked contact: ${id}`
        );
    } catch (err) {
        console.error(
            '[DB] Failed to remove tracked contact:',
            err
        );
    }
}


// Get all saved tracked contacts
async function getTrackedContacts() {
    if (!pool) return [];

    try {
        const result = await pool.query(
            `
            SELECT
                id,
                platform,
                phone_number
            FROM tracked_contacts
            ORDER BY created_at ASC
            `
        );

        return result.rows;
    } catch (err) {
        console.error(
            '[DB] Failed to load tracked contacts:',
            err
        );

        return [];
    }
}


// ============================================================
// WEB SERVER
// ============================================================

const clientPath = path.join(
    process.cwd(),
    'client',
    'build'
);

app.use(
    express.static(clientPath)
);

const httpServer =
    createServer(app);

const io = new Server(
    httpServer,
    {
        cors: {
            origin: "*",
            methods: ["GET", "POST"]
        },
        transports: [
            'websocket',
            'polling'
        ]
    }
);


// ============================================================
// STATE
// ============================================================

let sock: any;

let isWhatsAppConnected =
    false;

let isSignalConnected =
    false;

let signalAccountNumber:
    string | null = null;

let globalProbeMethod:
    ProbeMethod = 'delete';

let currentWhatsAppQr:
    string | null = null;

type Platform =
    'whatsapp' |
    'signal';

interface TrackerEntry {
    tracker:
        WhatsAppTracker |
        SignalTracker;

    platform:
        Platform;
}

const trackers:
    Map<string, TrackerEntry> =
    new Map();


// Prevent restoring the same contacts
// multiple times after reconnect events.
let whatsappRestoreInProgress =
    false;

let signalRestoreInProgress =
    false;


// ============================================================
// TRACKER RESTORATION
// ============================================================

async function restoreWhatsAppTrackers() {
    if (
        !isWhatsAppConnected ||
        !sock ||
        whatsappRestoreInProgress
    ) {
        return;
    }

    if (!pool) {
        return;
    }

    whatsappRestoreInProgress =
        true;

    try {
        const savedContacts =
            await getTrackedContacts();

        const whatsappContacts =
            savedContacts.filter(
                contact =>
                    contact.platform ===
                    'whatsapp'
            );

        console.log(
            `[RESTORE] Found ${whatsappContacts.length} saved WhatsApp tracker(s)`
        );

        for (
            const contact of whatsappContacts
        ) {
            if (
                trackers.has(contact.id)
            ) {
                continue;
            }

            try {
                const results =
                    await sock.onWhatsApp(
                        contact.id
                    );

                const result =
                    results?.[0];

                if (!result?.exists) {
                    console.log(
                        `[RESTORE] WhatsApp number no longer exists: ${contact.id}`
                    );

                    continue;
                }

                const tracker =
                    new WhatsAppTracker(
                        sock,
                        result.jid
                    );

                tracker.setProbeMethod(
                    globalProbeMethod
                );

                trackers.set(
                    result.jid,
                    {
                        tracker,
                        platform:
                            'whatsapp'
                    }
                );

                tracker.onUpdate =
                    (updateData) => {
                        void saveMeasurement(
                            result.jid,
                            updateData
                        );

                        io.emit(
                            'tracker-update',
                            {
                                jid: result.jid,
                                platform:
                                    'whatsapp',
                                ...updateData
                            }
                        );
                    };

                tracker.startTracking();

                console.log(
                    `[RESTORE] Restored WhatsApp tracker: ${result.jid}`
                );

            } catch (err) {
                console.error(
                    `[RESTORE] Failed to restore WhatsApp tracker ${contact.id}:`,
                    err
                );
            }
        }

        io.emit(
            'tracked-contacts',
            Array.from(
                trackers.entries()
            ).map(
                ([id, entry]) => ({
                    id,
                    platform:
                        entry.platform
                })
            )
        );

    } catch (err) {
        console.error(
            '[RESTORE] Failed to restore WhatsApp trackers:',
            err
        );
    } finally {
        whatsappRestoreInProgress =
            false;
    }
}


async function restoreSignalTrackers() {
    if (
        !isSignalConnected ||
        !signalAccountNumber ||
        signalRestoreInProgress
    ) {
        return;
    }

    if (!pool) {
        return;
    }

    signalRestoreInProgress =
        true;

    try {
        const savedContacts =
            await getTrackedContacts();

        const signalContacts =
            savedContacts.filter(
                contact =>
                    contact.platform ===
                    'signal'
            );

        console.log(
            `[RESTORE] Found ${signalContacts.length} saved Signal tracker(s)`
        );

        for (
            const contact of signalContacts
        ) {
            if (
                trackers.has(contact.id)
            ) {
                continue;
            }

            try {
                const cleanNumber =
                    contact.phone_number
                        .replace(/\D/g, '');

                const targetNumber =
                    `+${cleanNumber}`;

                const signalId =
                    `signal:${cleanNumber}`;

                const checkResult =
                    await checkSignalNumber(
                        SIGNAL_API_URL,
                        signalAccountNumber,
                        targetNumber
                    );

                if (
                    !checkResult.registered
                ) {
                    console.log(
                        `[RESTORE] Signal number is not registered: ${targetNumber}`
                    );

                    continue;
                }

                const tracker =
                    new SignalTracker(
                        SIGNAL_API_URL,
                        signalAccountNumber,
                        targetNumber
                    );

                trackers.set(
                    signalId,
                    {
                        tracker,
                        platform:
                            'signal'
                    }
                );

                tracker.onUpdate =
                    (updateData) => {
                        void saveMeasurement(
                            signalId,
                            updateData
                        );

                        io.emit(
                            'tracker-update',
                            {
                                jid: signalId,
                                platform:
                                    'signal',
                                ...updateData
                            }
                        );
                    };

                tracker.startTracking();

                console.log(
                    `[RESTORE] Restored Signal tracker: ${signalId}`
                );

            } catch (err) {
                console.error(
                    `[RESTORE] Failed to restore Signal tracker ${contact.id}:`,
                    err
                );
            }
        }

        io.emit(
            'tracked-contacts',
            Array.from(
                trackers.entries()
            ).map(
                ([id, entry]) => ({
                    id,
                    platform:
                        entry.platform
                })
            )
        );

    } catch (err) {
        console.error(
            '[RESTORE] Failed to restore Signal trackers:',
            err
        );
    } finally {
        signalRestoreInProgress =
            false;
    }
}


// ============================================================
// WHATSAPP CONNECTION
// ============================================================

async function connectToWhatsApp() {
    const {
        state,
        saveCreds
    } =
        await useMultiFileAuthState(
            'auth_info_baileys'
        );

    sock =
        makeWASocket({
            auth: state,
            logger: pino({
                level: 'debug'
            }),
            markOnlineOnConnect: true,
            printQRInTerminal: false,
        });

    sock.ev.on(
        'connection.update',
        async (update: any) => {
            const {
                connection,
                lastDisconnect,
                qr
            } = update;

            if (qr) {
                console.log(
                    'QR Code generated'
                );

                currentWhatsAppQr =
                    qr;

                io.emit(
                    'qr',
                    qr
                );
            }

            if (
                connection ===
                'close'
            ) {
                isWhatsAppConnected =
                    false;

                currentWhatsAppQr =
                    null;

                const shouldReconnect =
                    (
                        lastDisconnect
                            ?.error as Boom
                    )?.output
                        ?.statusCode !==
                    DisconnectReason.loggedOut;

                console.log(
                    'connection closed, reconnecting ',
                    shouldReconnect
                );

                if (
                    shouldReconnect
                ) {
                    connectToWhatsApp();
                }

            } else if (
                connection ===
                'open'
            ) {
                isWhatsAppConnected =
                    true;

                currentWhatsAppQr =
                    null;

                console.log(
                    'opened connection'
                );

                io.emit(
                    'connection-open'
                );

                // Restore saved trackers
                // after WhatsApp connects.
                await restoreWhatsAppTrackers();
            }
        }
    );

    sock.ev.on(
        'creds.update',
        saveCreds
    );

    sock.ev.on(
        'messaging-history.set',
        ({
            chats,
            contacts,
            messages,
            isLatest
        }: any) => {
            console.log(
                `[SESSION] History sync - Chats: ${chats.length}, Contacts: ${contacts.length}, Messages: ${messages.length}, Latest: ${isLatest}`
            );
        }
    );

    sock.ev.on(
        'messages.update',
        (updates: any) => {
            for (
                const update of updates
            ) {
                console.log(
                    `[MSG UPDATE] JID: ${update.key.remoteJid}, ID: ${update.key.id}, Status: ${update.update.status}, FromMe: ${update.key.fromMe}`
                );
            }
        }
    );
}

connectToWhatsApp();


// ============================================================
// SIGNAL
// ============================================================

let signalLinkingInProgress =
    false;

let signalApiAvailable =
    false;

let currentSignalQrUrl:
    string | null = null;


async function checkSignalApiAvailable(): Promise<boolean> {
    try {
        const response =
            await fetch(
                `${SIGNAL_API_URL}/v1/about`,
                {
                    signal:
                        AbortSignal.timeout(
                            2000
                        )
                }
            );

        return response.ok;

    } catch {
        return false;
    }
}


async function checkSignalConnection() {
    try {
        const available =
            await checkSignalApiAvailable();

        if (
            available !==
            signalApiAvailable
        ) {
            signalApiAvailable =
                available;

            console.log(
                `[SIGNAL] API available: ${available}`
            );

            io.emit(
                'signal-api-status',
                {
                    available
                }
            );
        }

        if (!available) {
            if (
                isSignalConnected
            ) {
                isSignalConnected =
                    false;

                signalAccountNumber =
                    null;

                io.emit(
                    'signal-disconnected'
                );
            }

            return;
        }

        const accounts =
            await getSignalAccounts(
                SIGNAL_API_URL
            );

        if (
            accounts.length > 0
        ) {
            if (
                !isSignalConnected
            ) {
                isSignalConnected =
                    true;

                signalAccountNumber =
                    accounts[0];

                signalLinkingInProgress =
                    false;

                console.log(
                    `[SIGNAL] Connected with account: ${signalAccountNumber}`
                );

                io.emit(
                    'signal-connection-open',
                    {
                        number:
                            signalAccountNumber
                    }
                );
            }

            // Restore saved Signal trackers.
            await restoreSignalTrackers();

        } else {
            if (
                isSignalConnected
            ) {
                isSignalConnected =
                    false;

                signalAccountNumber =
                    null;

                console.log(
                    '[SIGNAL] Disconnected'
                );

                io.emit(
                    'signal-disconnected'
                );
            }

            if (
                !signalLinkingInProgress
            ) {
                startSignalLinking();
            }
        }

    } catch (err) {
        console.log(
            '[SIGNAL] Error checking connection:',
            err
        );

        if (
            isSignalConnected
        ) {
            isSignalConnected =
                false;

            signalAccountNumber =
                null;

            io.emit(
                'signal-disconnected'
            );
        }
    }
}


async function startSignalLinking() {
    if (
        signalLinkingInProgress ||
        isSignalConnected
    ) {
        return;
    }

    signalLinkingInProgress =
        true;

    console.log(
        '[SIGNAL] Starting device linking...'
    );

    try {
        const response =
            await fetch(
                `${SIGNAL_API_URL}/v1/qrcodelink?device_name=activity-tracker`
            );

        if (!response.ok) {
            console.log(
                '[SIGNAL] Failed to start linking:',
                response.status
            );

            signalLinkingInProgress =
                false;

            return;
        }

        currentSignalQrUrl =
            `${SIGNAL_API_URL}/v1/qrcodelink?device_name=activity-tracker&t=${Date.now()}`;

        console.log(
            '[SIGNAL] Emitting QR image URL:',
            currentSignalQrUrl
        );

        io.emit(
            'signal-qr-image',
            currentSignalQrUrl
        );

        pollSignalLinkingStatus();

    } catch (err) {
        console.log(
            '[SIGNAL] Error starting linking:',
            err
        );

        signalLinkingInProgress =
            false;
    }
}


async function pollSignalLinkingStatus() {
    const checkInterval =
        setInterval(
            async () => {
                try {
                    const accounts =
                        await getSignalAccounts(
                            SIGNAL_API_URL
                        );

                    if (
                        accounts.length > 0
                    ) {
                        clearInterval(
                            checkInterval
                        );

                        signalLinkingInProgress =
                            false;

                        currentSignalQrUrl =
                            null;

                        isSignalConnected =
                            true;

                        signalAccountNumber =
                            accounts[0];

                        console.log(
                            `[SIGNAL] Linking completed! Account: ${signalAccountNumber}`
                        );

                        io.emit(
                            'signal-connection-open',
                            {
                                number:
                                    signalAccountNumber
                            }
                        );

                        await restoreSignalTrackers();
                    }

                } catch {
                    // Keep polling
                }
            },
            2000
        );

    setTimeout(
        () => {
            clearInterval(
                checkInterval
            );

            signalLinkingInProgress =
                false;
        },
        300000
    );
}


checkSignalConnection();

setInterval(
    checkSignalConnection,
    5000
);


// ============================================================
// SOCKET AUTHENTICATION
// ============================================================

io.use(
    (socket, next) => {
        const authHeader =
            socket.handshake.auth
                ?.token;

        if (
            !authHeader ||
            !authHeader.startsWith(
                'Basic '
            )
        ) {
            console.log(
                '[AUTH ERROR] Missing or invalid credentials configuration format'
            );

            return next(
                new Error(
                    'Authentication error'
                )
            );
        }

        const base64Credentials =
            authHeader.split(
                ' '
            )[1];

        const decoded =
            Buffer
                .from(
                    base64Credentials,
                    'base64'
                )
                .toString(
                    'utf8'
                );

        const [
            username,
            password
        ] =
            decoded.split(':');

        const expectedPassword =
            process.env
                .DASHBOARD_PASSWORD ||
            'fallback_temporary_password';

        if (
            username === 'admin' &&
            password ===
                expectedPassword
        ) {
            return next();
        }

        console.log(
            '[AUTH ERROR] Password credentials mismatch'
        );

        return next(
            new Error(
                'Authentication error'
            )
        );
    }
);


// ============================================================
// SOCKET CONNECTION
// ============================================================

io.on(
    'connection',
    (socket) => {
        console.log(
            'Client connected'
        );

        if (
            currentWhatsAppQr
        ) {
            socket.emit(
                'qr',
                currentWhatsAppQr
            );
        }

        if (
            isWhatsAppConnected
        ) {
            socket.emit(
                'connection-open'
            );
        }

        if (
            isSignalConnected &&
            signalAccountNumber
        ) {
            socket.emit(
                'signal-connection-open',
                {
                    number:
                        signalAccountNumber
                }
            );
        }

        socket.emit(
            'signal-api-status',
            {
                available:
                    signalApiAvailable
            }
        );

        if (
            signalLinkingInProgress &&
            currentSignalQrUrl
        ) {
            socket.emit(
                'signal-qr-image',
                currentSignalQrUrl
            );
        }

        socket.emit(
            'probe-method',
            globalProbeMethod
        );


        // ====================================================
        // GET TRACKED CONTACTS
        // ====================================================

        socket.on(
            'get-tracked-contacts',
            async () => {
                const savedContacts =
                    await getTrackedContacts();

                // If the DB has saved contacts,
                // use those as the source of truth.
                if (
                    savedContacts.length > 0
                ) {
                    socket.emit(
                        'tracked-contacts',
                        savedContacts.map(
                            contact => ({
                                id:
                                    contact.id,
                                platform:
                                    contact.platform
                            })
                        )
                    );

                    return;
                }

                // Fallback to currently active
                // in-memory trackers.
                socket.emit(
                    'tracked-contacts',
                    Array.from(
                        trackers.entries()
                    ).map(
                        ([id, entry]) => ({
                            id,
                            platform:
                                entry.platform
                        })
                    )
                );
            }
        );


        // ====================================================
        // GET MEASUREMENT HISTORY
        // ====================================================

        socket.on(
            'get-measurement-history',
            async (
                data:
                    string |
                    {
                        jid: string;
                        limit?: number;
                    }
            ) => {
                const jid =
                    typeof data ===
                    'string'
                        ? data
                        : data?.jid;

                const limit =
                    typeof data ===
                    'string'
                        ? 500
                        : data?.limit ??
                          500;

                if (!jid) {
                    return;
                }

                const history =
                    await getMeasurementHistory(
                        jid,
                        limit
                    );

                socket.emit(
                    'measurement-history',
                    {
                        jid,
                        data: history
                    }
                );
            }
        );


        // ====================================================
        // ADD CONTACT
        // ====================================================

        socket.on(
            'add-contact',
            async (
                data:
                    string |
                    {
                        number: string;
                        platform: Platform;
                    }
            ) => {
                const {
                    number,
                    platform
                } =
                    typeof data ===
                    'string'
                        ? {
                            number: data,
                            platform:
                                'whatsapp' as Platform
                        }
                        : data;

                const cleanNumber =
                    number.replace(
                        /\D/g,
                        ''
                    );


                // ============================================
                // SIGNAL
                // ============================================

                if (
                    platform ===
                    'signal'
                ) {
                    if (
                        !isSignalConnected ||
                        !signalAccountNumber
                    ) {
                        socket.emit(
                            'error',
                            {
                                message:
                                    'Signal is not connected. Please link Signal first.'
                            }
                        );

                        return;
                    }

                    const signalId =
                        `signal:${cleanNumber}`;

                    if (
                        trackers.has(
                            signalId
                        )
                    ) {
                        socket.emit(
                            'error',
                            {
                                jid:
                                    signalId,
                                message:
                                    'Already tracking this contact on Signal'
                            }
                        );

                        return;
                    }

                    try {
                        const targetNumber =
                            `+${cleanNumber}`;

                        const checkResult =
                            await checkSignalNumber(
                                SIGNAL_API_URL,
                                signalAccountNumber,
                                targetNumber
                            );

                        if (
                            !checkResult.registered
                        ) {
                            socket.emit(
                                'error',
                                {
                                    jid:
                                        signalId,
                                    message:
                                        checkResult.error ||
                                        'Number is not discoverable on Signal'
                                }
                            );

                            return;
                        }

                        const tracker =
                            new SignalTracker(
                                SIGNAL_API_URL,
                                signalAccountNumber,
                                targetNumber
                            );

                        trackers.set(
                            signalId,
                            {
                                tracker,
                                platform:
                                    'signal'
                            }
                        );

                        tracker.onUpdate =
                            (
                                updateData
                            ) => {
                                void saveMeasurement(
                                    signalId,
                                    updateData
                                );

                                io.emit(
                                    'tracker-update',
                                    {
                                        jid:
                                            signalId,
                                        platform:
                                            'signal',
                                        ...updateData
                                    }
                                );
                            };

                        tracker.startTracking();

                        // Save the contact
                        // permanently.
                        await saveTrackedContact(
                            signalId,
                            'signal',
                            cleanNumber
                        );

                        socket.emit(
                            'contact-added',
                            {
                                jid:
                                    signalId,
                                number:
                                    cleanNumber,
                                platform:
                                    'signal'
                            }
                        );

                        io.emit(
                            'contact-name',
                            {
                                jid:
                                    signalId,
                                name:
                                    cleanNumber
                            }
                        );

                    } catch (err) {
                        console.error(
                            err
                        );

                        socket.emit(
                            'error',
                            {
                                message:
                                    'Failed to start Signal tracking'
                            }
                        );
                    }

                    return;
                }


                // ============================================
                // WHATSAPP
                // ============================================

                if (!sock) {
                    socket.emit(
                        'error',
                        {
                            message:
                                'WhatsApp is not ready yet'
                        }
                    );

                    return;
                }

                const targetJid =
                    `${cleanNumber}@s.whatsapp.net`;

                if (
                    trackers.has(
                        targetJid
                    )
                ) {
                    socket.emit(
                        'error',
                        {
                            jid:
                                targetJid,
                            message:
                                'Already tracking this contact'
                        }
                    );

                    return;
                }

                try {
                    const results =
                        await sock.onWhatsApp(
                            targetJid
                        );

                    const result =
                        results?.[0];

                    if (
                        !result?.exists
                    ) {
                        socket.emit(
                            'error',
                            {
                                jid:
                                    targetJid,
                                message:
                                    'Number not on WhatsApp'
                            }
                        );

                        return;
                    }

                    const tracker =
                        new WhatsAppTracker(
                            sock,
                            result.jid
                        );

                    tracker.setProbeMethod(
                        globalProbeMethod
                    );

                    trackers.set(
                        result.jid,
                        {
                            tracker,
                            platform:
                                'whatsapp'
                        }
                    );

                    tracker.onUpdate =
                        (
                            updateData
                        ) => {
                            void saveMeasurement(
                                result.jid,
                                updateData
                            );

                            io.emit(
                                'tracker-update',
                                {
                                    jid:
                                        result.jid,
                                    platform:
                                        'whatsapp',
                                    ...updateData
                                }
                            );
                        };

                    tracker.startTracking();


                    const ppUrl =
                        await tracker.getProfilePicture();

                    let contactName =
                        cleanNumber;

                    try {
                        const contactInfo =
                            await sock.onWhatsApp(
                                result.jid
                            );

                        if (
                            contactInfo?.[0]
                                ?.notify
                        ) {
                            contactName =
                                contactInfo[0]
                                    .notify;
                        }
                    } catch {
                        console.log(
                            '[NAME] Could not fetch contact name, using number'
                        );
                    }


                    // Save the contact
                    // permanently.
                    await saveTrackedContact(
                        result.jid,
                        'whatsapp',
                        cleanNumber
                    );


                    socket.emit(
                        'contact-added',
                        {
                            jid:
                                result.jid,
                            number:
                                cleanNumber,
                            platform:
                                'whatsapp'
                        }
                    );

                    io.emit(
                        'profile-pic',
                        {
                            jid:
                                result.jid,
                            url:
                                ppUrl
                        }
                    );

                    io.emit(
                        'contact-name',
                        {
                            jid:
                                result.jid,
                            name:
                                contactName
                        }
                    );

                } catch (err) {
                    console.error(
                        err
                    );

                    socket.emit(
                        'error',
                        {
                            jid:
                                targetJid,
                            message:
                                'Verification failed'
                        }
                    );
                }
            }
        );


        // ====================================================
        // REMOVE CONTACT
        // ====================================================

        socket.on(
            'remove-contact',
            async (
                jid: string
            ) => {
                const entry =
                    trackers.get(jid);

                if (entry) {
                    entry.tracker
                        .stopTracking();

                    trackers.delete(
                        jid
                    );
                }

                // Remove from PostgreSQL
                await removeTrackedContact(
                    jid
                );

                socket.emit(
                    'contact-removed',
                    jid
                );
            }
        );


        // ====================================================
        // PROBE METHOD
        // ====================================================

        socket.on(
            'set-probe-method',
            (
                method: ProbeMethod
            ) => {
                if (
                    method !==
                        'delete' &&
                    method !==
                        'reaction'
                ) {
                    socket.emit(
                        'error',
                        {
                            message:
                                'Invalid probe method'
                        }
                    );

                    return;
                }

                globalProbeMethod =
                    method;

                for (
                    const entry of
                    trackers.values()
                ) {
                    if (
                        entry.platform ===
                        'whatsapp'
                    ) {
                        (
                            entry.tracker as
                                WhatsAppTracker
                        ).setProbeMethod(
                            method
                        );
                    }
                }

                io.emit(
                    'probe-method',
                    method
                );
            }
        );


        socket.on(
            'disconnect',
            () => {
                console.log(
                    'Client disconnected'
                );
            }
        );
    }
);


// ============================================================
// REACT SPA FALLBACK
// ============================================================

app.get(
    '/{*path}',
    (req, res) => {
        res.sendFile(
            path.join(
                clientPath,
                'index.html'
            )
        );
    }
);


// ============================================================
// START SERVER
// ============================================================

const PORT =
    process.env.PORT ||
    3001;

httpServer.listen(
    PORT,
    () => {
        console.log(
            `Server running on port ${PORT}`
        );
    }
);