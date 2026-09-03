import React, { useState, useEffect } from 'react';
import { io, Socket } from 'socket.io-client';
// Keep all your original imports exactly as they are below this line...

// 1. Force the password box check on your phone screen once
let savedPassword = localStorage.getItem('tracker_password');
if (!savedPassword) {
    const userInput = prompt('Enter your dashboard security password:');
    if (userInput) {
        localStorage.setItem('tracker_password', userInput);
        savedPassword = userInput;
    }
}

// 2. Base64 encode the saved credential token safely
const encodeAuth = (user: string, pass: string) => {
    try {
        return 'Basic ' + btoa(unescape(encodeURIComponent(user + ':' + pass)));
    } catch (e) {
        return '';
    }
};

// 3. Force the connection straight to your live Render link with fallbacks
const API_URL = 'https://onrender.com';
export const socket: Socket = io(API_URL, {
    transports: ['websocket', 'polling'],
    autoConnect: true,
    auth: {
        token: encodeAuth('admin', savedPassword || '')
    }
});


// 2. Main Visual Layout Application Interface
export default function App() {
    const [connected, setConnected] = useState(false);
    const [qrCode, setQrCode] = useState<string | null>(null);
    const [targetNumber, setTargetNumber] = useState('');
    const [trackedDevices, setTrackedDevices] = useState<string[]>([]);

    useEffect(() => {
        socket.on('connect', () => setConnected(true));
        socket.on('disconnect', () => setConnected(false));
        socket.on('qr', (qr: string) => setQrCode(qr));
        socket.on('connection-open', () => {
            setQrCode(null);
            setConnected(true);
        });

        return () => {
            socket.off('connect');
            socket.off('disconnect');
            socket.off('qr');
            socket.off('connection-open');
        };
    }, []);

    const handleAddDevice = (e: React.FormEvent) => {
        e.preventDefault();
        if (!targetNumber) return;
        setTrackedDevices([...trackedDevices, targetNumber]);
        setTargetNumber('');
    };

    const handleRemoveDevice = (num: string) => {
        setTrackedDevices(trackedDevices.filter(d => d !== num));
    };

    return (
        <div style={{ padding: '20px', fontFamily: 'sans-serif', maxWidth: '600px', margin: '0 auto', backgroundColor: '#f9fafb' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
                <h1 style={{ fontSize: '24px', fontWeight: 'bold', color: '#111827' }}>Activity Tracker</h1>
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '14px' }}>
                    <span style={{ width: '10px', height: '10px', borderRadius: '50%', backgroundColor: connected ? '#10b981' : '#ef4444' }}></span>
                    <span style={{ color: '#4b5563' }}>{connected ? 'Connected' : 'Disconnected'}</span>
                </div>
            </div>

            <div style={{ backgroundColor: '#fff', padding: '24px', borderRadius: '12px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)', marginBottom: '20px', textAlign: 'center' }}>
                <h2 style={{ fontSize: '18px', fontWeight: '600', marginBottom: '16px' }}>Connect WhatsApp</h2>
                
                {qrCode ? (
                    <div style={{ margin: '20px auto', padding: '10px', backgroundColor: '#fff', display: 'inline-block', borderRadius: '8px', border: '1px solid #e5e7eb' }}>
                        <QRCodeSVG value={qrCode} size={200} />
                    </div>
                ) : (
                    <div style={{ height: '200px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#9ca3af', border: '2px dashed #e5e7eb', borderRadius: '8px', marginBottom: '16px' }}>
                        {connected ? 'Waiting for Handshake QR...' : 'Connecting to Server Gateway...'}
                    </div>
                )}
                <p style={{ fontSize: '13px', color: '#6b7280' }}>Open WhatsApp on your mobile phone, go to Settings &gt; Linked Devices, and scan this visual configuration code pattern.</p>
            </div>

            <div style={{ backgroundColor: '#fff', padding: '24px', borderRadius: '12px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
                <h2 style={{ fontSize: '18px', fontWeight: '600', marginBottom: '16px' }}>Monitor Target Details</h2>
                <form onSubmit={handleAddDevice} style={{ display: 'flex', gap: '8px', marginBottom: '20px' }}>
                    <input 
                        type="text" 
                        placeholder="Format: 447123456789" 
                        value={targetNumber}
                        onChange={(e) => setTargetNumber(e.target.value)}
                        style={{ flex: 1, padding: '10px 14px', border: '1px solid #d1d5db', borderRadius: '8px', fontSize: '14px' }}
                    />
                    <button type="submit" style={{ padding: '10px 16px', backgroundColor: '#2563eb', color: '#fff', border: 'none', borderRadius: '8px', fontWeight: '500', cursor: 'pointer' }}>Add Target</button>
                </form>

                <div>
                    {trackedDevices.map(num => (
                        <div key={num} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '12px', borderBottom: '1px solid #f3f4f6' }}>
                            <span style={{ fontSize: '15px', color: '#374151', fontWeight: '500' }}>+{num}</span>
                            <button onClick={() => handleRemoveDevice(num)} style={{ color: '#ef4444', background: 'none', border: 'none', cursor: 'pointer', padding: '4px' }}>Remove</button>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
