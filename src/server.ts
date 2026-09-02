/**
 * Device Activity Tracker - Web Server
 *
 * HTTP server with Socket.IO for real-time tracking visualization.
 * Provides REST API and WebSocket interface for the React frontend.
 *
 * For educational and research purposes only.
 */

import express from 'express';
import basicAuth from 'express-basic-auth'; // 👈 Add this line
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import path from 'path';
import makeWASocket, { DisconnectReason, useMultiFileAuthState } from '@whiskeysockets/baileys';
import { pino } from 'pino';
import { Boom } from '@hapi/boom';
import { WhatsAppTracker, ProbeMethod } from './tracker.js';
import { SignalTracker, getSignalAccounts, checkSignalNumber } from './signal-tracker.js';

// Configuration
const SIGNAL_API_URL = process.env.SIGNAL_API_URL || 'http://localhost:8080';

const app = express();
app.use(cors());

// Serve static React web files out of the client/build directory
const clientPath = path.join(process.cwd(), 'client', 'build');
app.use(express.static(clientPath));

const httpServer = createServer(app);
const io = new Server(httpServer, {
    cors: {
        origin: "*", // Allow all origins for dev
        methods: ["GET", "POST"]
    },
    transports: ['websocket', 'polling'] // 👈 Add this line to fix Render connection routing
});


let sock: any;
let isWhatsAppConnected = false;
let isSignalConnected = false;
let signalAccountNumber: string | null = null;
let globalProbeMethod: ProbeMethod = 'delete'; // Default to delete method
let currentWhatsAppQr: string | null = null; // Store current QR code for new clients

// Platform type for contacts
type Platform = 'whatsapp' | 'signal';

interface TrackerEntry {
    tracker: WhatsAppTracker | SignalTracker;
    platform: Platform;
}

const trackers: Map<string, TrackerEntry> = new Map(); // JID/Number -> Tracker entry

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
            currentWhatsAppQr = qr; // Store the QR code
            io.emit('qr', qr);
        }

        if (connection === 'close') {
            isWhatsAppConnected = false;
            currentWhatsAppQr = null; // Clear QR on close
            const shouldReconnect = (lastDisconnect?.error as Boom)?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log('connection closed, reconnecting ', shouldReconnect);
            if (shouldReconnect) {
                connectToWhatsApp();
            }
        } else if (connection === 'open') {
            isWhatsAppConnected = true;
            currentWhatsAppQr = null; // Clear QR on successful connection
            console.log('opened connection');
            io.emit('connection-open');
        }
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('messaging-history.set', ({ chats, contacts, messages, isLatest }: any) => {
        console.log(`[SESSION] History sync - Chats: ${chats.length}, Contacts: ${contacts.length}, Messages: ${messages.length}, Latest: ${isLatest}`);
    });

    sock.ev.on('messages.update', (updates: any) => {
        for (const update of updates) {
            console.log(`[MSG UPDATE] JID: ${update.key.remoteJid}, ID: ${update.key.id}, Status: ${update.update.status}, FromMe: ${update.key.fromMe}`);
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
                console.log(`[SIGNAL] Connected with account: ${signalAccountNumber}`);
                io.emit('signal-connection-open', { number: signalAccountNumber });
            }
        } else {
            if (isSignalConnected) {
                isSignalConnected = false;
                signalAccountNumber = null;
                console.log('[SIGNAL] Disconnected');
                io.emit('signal-disconnected');
            }
            // No accounts - need to link, start QR code process
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

// Start Signal device linking - signal-cli-rest-api returns QR as PNG image
async function startSignalLinking() {
    if (signalLinkingInProgress || isSignalConnected) return;

    signalLinkingInProgress = true;
    console.log('[SIGNAL] Starting device linking...');

    try {
        const response = await fetch(`${SIGNAL_API_URL}/v1/qrcodelink?device_name=activity-tracker`);
        if (!response.ok) {
            console.log('[SIGNAL] Failed to start linking:', response.status);
            signalLinkingInProgress = false;
            return;
        }

        currentSignalQrUrl = `${SIGNAL_API_URL}/v1/qrcodelink?device_name=activity-tracker&t=${Date.now()}`;
        console.log('[SIGNAL] Emitting QR image URL:', currentSignalQrUrl);
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
                console.log(`[SIGNAL] Linking completed! Account: ${signalAccountNumber}`);
                io.emit('signal-connection-open', { number: signalAccountNumber });
            }
        } catch (err) {
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

// Establish WebSocket listeners
io.on('connection', (socket) => {
    console.log('Client connected');

    if (currentWhatsAppQr) {
        socket.emit('qr', currentWhatsAppQr);
    }

    if (isWhatsAppConnected) {
        socket.emit('connection-open');
    }

    if (isSignalConnected) {
        socket.emit('signal-connection-open', { number: signalAccountNumber });
    }

    socket.on('disconnect', () => {
        console.log('Client disconnected');
    });
});

// Fallback directory path configuration for React Single Page Application routing
app.get('/*path', (req, res) => {
    res.sendFile(path.join(clientPath, 'index.html'));
});

// Start web server engine
const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
