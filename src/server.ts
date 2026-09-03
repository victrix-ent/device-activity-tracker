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
import makeWASocket, { DisconnectReason, useMultiFileAuthState } from '@whiskeysockets/baileys';
import { pino } from 'pino';
import { Boom } from '@hapi/boom';
import { Pool } from 'pg';
import { WhatsAppTracker, ProbeMethod } from './tracker.js';
import { SignalTracker, getSignalAccounts, checkSignalNumber } from './signal-tracker.js';

// Configuration
const SIGNAL_API_URL = process.env.SIGNAL_API_URL || 'http://localhost:8080';

const app = express();
app.use(cors());

// PostgreSQL
const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
    console.warn('[DB] DATABASE_URL is not configured. Measurements will not be persisted.');
}

const pool = databaseUrl
    ? new Pool({
        connectionString: databaseUrl,
        ssl: { rejectUnauthorized: false },
    })
    : null;

if (pool) {
    pool.on('error', (err) => {
        console.error('[DB] Unexpected PostgreSQL pool error:', err);
    });
}

// Save a tracker measurement to PostgreSQL
async function saveMeasurement(jid: string, updateData: any) {
    if (!pool) return;

    try {
        const device = updateData.devices?.[0];

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
        console.error('[DB] Failed to save measurement:', err);
    }
}

// Load historical measurements for a tracked device
async function getMeasurementHistory(jid: string, limit = 500) {
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
        console.error('[DB] Failed to load measurement history:', err);
        return [];
    }
}


// Serve static React web files out of the client/build directory
const clientPath = path.join(process.cwd(), 'client', 'build');
app.use(express.static(clientPath));

const httpServer = createServer(app);

const io = new Server(httpServer, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    },
    transports: ['websocket', 'polling']
});


let sock: any;
let isWhatsAppConnected = false;
let isSignalConnected = false;
let signalAccountNumber: string | null = null;
let globalProbeMethod: ProbeMethod = 'delete';
let currentWhatsAppQr: string | null = null;

// Platform type for contacts
type Platform = 'whatsapp' | 'signal';

interface TrackerEntry {
    tracker: WhatsAppTracker | SignalTracker;
    platform: Platform;
}

const trackers: Map<string, TrackerEntry> = new Map();


async function connectToWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');

    sock = makeWASocket({
        auth: state,
        logger: pino({ level: 'debug' }),
        markOnlineOnConnect: true,
        printQRInTerminal: false,
    });

    sock.ev.on('connection.update', async (update: any) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            console.log('QR Code generated');
            currentWhatsAppQr = qr;
            io.emit('qr', qr);
        }

        if (connection === 'close') {
            isWhatsAppConnected = false;
            currentWhatsAppQr = null;

            const shouldReconnect =
                (lastDisconnect?.error as Boom)?.output?.statusCode !==
                DisconnectReason.loggedOut;

            console.log('connection closed, reconnecting ', shouldReconnect);

            if (shouldReconnect) {
                connectToWhatsApp();
            }
        } else if (connection === 'open') {
            isWhatsAppConnected = true;
            currentWhatsAppQr = null;

            console.log('opened connection');
            io.emit('connection-open');
        }
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on(
        'messaging-history.set',
        ({ chats, contacts, messages, isLatest }: any) => {
            console.log(
                `[SESSION] History sync - Chats: ${chats.length}, Contacts: ${contacts.length}, Messages: ${messages.length}, Latest: ${isLatest}`
            );
        }
    );

    sock.ev.on('messages.update', (updates: any) => {
        for (const update of updates) {
            console.log(
                `[MSG UPDATE] JID: ${update.key.remoteJid}, ID: ${update.key.id}, Status: ${update.update.status}, FromMe: ${update.key.fromMe}`
            );
        }
    });
}

connectToWhatsApp();


// Signal linking state
let signalLinkingInProgress = false;
let signalApiAvailable = false;
let currentSignalQrUrl: string | null = null;


// Check Signal API availability
async function checkSignalApiAvailable(): Promise<boolean> {
    try {
        const response = await fetch(`${SIGNAL_API_URL}/v1/about`, {
            signal: AbortSignal.timeout(2000)
        });

        return response.ok;
    } catch {
        return false;
    }
}


// Check Signal connection status
async function checkSignalConnection() {
    try {
        const available = await checkSignalApiAvailable();

        if (available !== signalApiAvailable) {
            signalApiAvailable = available;

            console.log(`[SIGNAL] API available: ${available}`);

            io.emit('signal-api-status', { available });
        }

        if (!available) {
            if (isSignalConnected) {
                isSignalConnected = false;
                signalAccountNumber = null;

                io.emit('signal-disconnected');
            }

            return;
        }

        const accounts = await getSignalAccounts(SIGNAL_API_URL);

        if (accounts.length > 0) {
            if (!isSignalConnected) {
                isSignalConnected = true;
                signalAccountNumber = accounts[0];
                signalLinkingInProgress = false;

                console.log(
                    `[SIGNAL] Connected with account: ${signalAccountNumber}`
                );

                io.emit('signal-connection-open', {
                    number: signalAccountNumber
                });
            }
        } else {
            if (isSignalConnected) {
                isSignalConnected = false;
                signalAccountNumber = null;

                console.log('[SIGNAL] Disconnected');

                io.emit('signal-disconnected');
            }

            if (!signalLinkingInProgress) {
                startSignalLinking();
            }
        }
    } catch (err) {
        console.log('[SIGNAL] Error checking connection:', err);

        if (isSignalConnected) {
            isSignalConnected = false;
            signalAccountNumber = null;

            io.emit('signal-disconnected');
        }
    }
}


// Start Signal device linking
async function startSignalLinking() {
    if (signalLinkingInProgress || isSignalConnected) return;

    signalLinkingInProgress = true;

    console.log('[SIGNAL] Starting device linking...');

    try {
        const response = await fetch(
            `${SIGNAL_API_URL}/v1/qrcodelink?device_name=activity-tracker`
        );

        if (!response.ok) {
            console.log(
                '[SIGNAL] Failed to start linking:',
                response.status
            );

            signalLinkingInProgress = false;

            return;
        }

        currentSignalQrUrl =
            `${SIGNAL_API_URL}/v1/qrcodelink?device_name=activity-tracker&t=${Date.now()}`;

        console.log(
            '[SIGNAL] Emitting QR image URL:',
            currentSignalQrUrl
        );

        io.emit('signal-qr-image', currentSignalQrUrl);

        pollSignalLinkingStatus();
    } catch (err) {
        console.log('[SIGNAL] Error starting linking:', err);

        signalLinkingInProgress = false;
    }
}


// Poll to check if Signal linking has completed
async function pollSignalLinkingStatus() {
    const checkInterval = setInterval(async () => {
        try {
            const accounts = await getSignalAccounts(SIGNAL_API_URL);

            if (accounts.length > 0) {
                clearInterval(checkInterval);

                signalLinkingInProgress = false;
                currentSignalQrUrl = null;
                isSignalConnected = true;
                signalAccountNumber = accounts[0];

                console.log(
                    `[SIGNAL] Linking completed! Account: ${signalAccountNumber}`
                );

                io.emit('signal-connection-open', {
                    number: signalAccountNumber
                });
            }
        } catch {
            // Keep polling
        }
    }, 2000);

    setTimeout(() => {
        clearInterval(checkInterval);
        signalLinkingInProgress = false;
    }, 300000);
}


// Check Signal connection periodically
checkSignalConnection();

setInterval(checkSignalConnection, 5000);


// Socket authentication
io.use((socket, next) => {
    const authHeader = socket.handshake.auth?.token;

    if (!authHeader || !authHeader.startsWith('Basic ')) {
        console.log(
            '[AUTH ERROR] Missing or invalid credentials configuration format'
        );

        return next(new Error('Authentication error'));
    }

    const base64Credentials = authHeader.split(' ')[1];

    const decoded = Buffer
        .from(base64Credentials, 'base64')
        .toString('utf8');

    const [username, password] = decoded.split(':');

    const expectedPassword =
        process.env.DASHBOARD_PASSWORD ||
        'fallback_temporary_password';

    if (username === 'admin' && password === expectedPassword) {
        return next();
    }

    console.log('[AUTH ERROR] Password credentials mismatch');

    return next(new Error('Authentication error'));
});


// Establish WebSocket listeners
io.on('connection', (socket) => {
    console.log('Client connected');

    if (currentWhatsAppQr) {
        socket.emit('qr', currentWhatsAppQr);
    }

    if (isWhatsAppConnected) {
        socket.emit('connection-open');
    }

    if (isSignalConnected && signalAccountNumber) {
        socket.emit('signal-connection-open', {
            number: signalAccountNumber
        });
    }

    socket.emit('signal-api-status', {
        available: signalApiAvailable
    });

    if (signalLinkingInProgress && currentSignalQrUrl) {
        socket.emit('signal-qr-image', currentSignalQrUrl);
    }

    socket.emit('probe-method', globalProbeMethod);


    // Return currently active trackers
    socket.on('get-tracked-contacts', () => {
        socket.emit(
            'tracked-contacts',
            Array.from(trackers.entries()).map(([id, entry]) => ({
                id,
                platform: entry.platform
            }))
        );
    });


    // Return historical measurements
    socket.on(
        'get-measurement-history',
        async (data: string | { jid: string; limit?: number }) => {
            const jid =
                typeof data === 'string'
                    ? data
                    : data?.jid;

            const limit =
                typeof data === 'string'
                    ? 500
                    : data?.limit ?? 500;

            if (!jid) return;

            const history = await getMeasurementHistory(jid, limit);

            socket.emit('measurement-history', {
                jid,
                data: history
            });
        }
    );


    // Add contact
    socket.on(
        'add-contact',
        async (
            data: string | {
                number: string;
                platform: Platform;
            }
        ) => {
            const { number, platform } =
                typeof data === 'string'
                    ? {
                        number: data,
                        platform: 'whatsapp' as Platform
                    }
                    : data;

            const cleanNumber = number.replace(/\D/g, '');


            // Signal
            if (platform === 'signal') {
                if (!isSignalConnected || !signalAccountNumber) {
                    socket.emit('error', {
                        message:
                            'Signal is not connected. Please link Signal first.'
                    });

                    return;
                }

                const signalId = `signal:${cleanNumber}`;

                if (trackers.has(signalId)) {
                    socket.emit('error', {
                        jid: signalId,
                        message:
                            'Already tracking this contact on Signal'
                    });

                    return;
                }

                try {
                    const targetNumber = `+${cleanNumber}`;

                    const checkResult =
                        await checkSignalNumber(
                            SIGNAL_API_URL,
                            signalAccountNumber,
                            targetNumber
                        );

                    if (!checkResult.registered) {
                        socket.emit('error', {
                            jid: signalId,
                            message:
                                checkResult.error ||
                                'Number is not discoverable on Signal'
                        });

                        return;
                    }

                    const tracker = new SignalTracker(
                        SIGNAL_API_URL,
                        signalAccountNumber,
                        targetNumber
                    );

                    trackers.set(signalId, {
                        tracker,
                        platform: 'signal'
                    });

                    tracker.onUpdate = (updateData) => {
                        void saveMeasurement(signalId, updateData);

                        io.emit('tracker-update', {
                            jid: signalId,
                            platform: 'signal',
                            ...updateData
                        });
                    };

                    tracker.startTracking();

                    socket.emit('contact-added', {
                        jid: signalId,
                        number: cleanNumber,
                        platform: 'signal'
                    });

                    io.emit('contact-name', {
                        jid: signalId,
                        name: cleanNumber
                    });

                } catch (err) {
                    console.error(err);

                    socket.emit('error', {
                        message:
                            'Failed to start Signal tracking'
                    });
                }

                return;
            }


            // WhatsApp
            const targetJid =
                `${cleanNumber}@s.whatsapp.net`;

            if (trackers.has(targetJid)) {
                socket.emit('error', {
                    jid: targetJid,
                    message:
                        'Already tracking this contact'
                });

                return;
            }

            try {
                const results =
                    await sock.onWhatsApp(targetJid);

                const result = results?.[0];

                if (!result?.exists) {
                    socket.emit('error', {
                        jid: targetJid,
                        message:
                            'Number not on WhatsApp'
                    });

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

                trackers.set(result.jid, {
                    tracker,
                    platform: 'whatsapp'
                });

                tracker.onUpdate = (updateData) => {
                    void saveMeasurement(
                        result.jid,
                        updateData
                    );

                    io.emit('tracker-update', {
                        jid: result.jid,
                        platform: 'whatsapp',
                        ...updateData
                    });
                };

                tracker.startTracking();


                const ppUrl =
                    await tracker.getProfilePicture();

                let contactName = cleanNumber;

                try {
                    const contactInfo =
                        await sock.onWhatsApp(
                            result.jid
                        );

                    if (contactInfo?.[0]?.notify) {
                        contactName =
                            contactInfo[0].notify;
                    }
                } catch {
                    console.log(
                        '[NAME] Could not fetch contact name, using number'
                    );
                }


                socket.emit('contact-added', {
                    jid: result.jid,
                    number: cleanNumber,
                    platform: 'whatsapp'
                });

                io.emit('profile-pic', {
                    jid: result.jid,
                    url: ppUrl
                });

                io.emit('contact-name', {
                    jid: result.jid,
                    name: contactName
                });

            } catch (err) {
                console.error(err);

                socket.emit('error', {
                    jid: targetJid,
                    message: 'Verification failed'
                });
            }
        }
    );


    // Remove contact
    socket.on('remove-contact', (jid: string) => {
        const entry = trackers.get(jid);

        if (entry) {
            entry.tracker.stopTracking();

            trackers.delete(jid);

            socket.emit('contact-removed', jid);
        }
    });


    // Change probe method
    socket.on(
        'set-probe-method',
        (method: ProbeMethod) => {
            if (
                method !== 'delete' &&
                method !== 'reaction'
            ) {
                socket.emit('error', {
                    message: 'Invalid probe method'
                });

                return;
            }

            globalProbeMethod = method;

            for (const entry of trackers.values()) {
                if (entry.platform === 'whatsapp') {
                    (
                        entry.tracker as WhatsAppTracker
                    ).setProbeMethod(method);
                }
            }

            io.emit(
                'probe-method',
                method
            );
        }
    );


    socket.on('disconnect', () => {
        console.log('Client disconnected');
    });
});


// Fallback directory path configuration for React Single Page Application routing
app.get('/{*path}', (req, res) => {
    res.sendFile(
        path.join(
            clientPath,
            'index.html'
        )
    );
});


// Start web server engine
const PORT =
    process.env.PORT || 3001;

httpServer.listen(PORT, () => {
    console.log(
        `Server running on port ${PORT}`
    );
});