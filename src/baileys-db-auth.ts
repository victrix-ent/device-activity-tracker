
import {
    AuthenticationState,
    AuthenticationCreds,
    SignalDataSet,
    SignalDataTypeMap,
    SignalKeyStore,
    initAuthCreds,
    BufferJSON,
    makeCacheableSignalKeyStore,
} from '@whiskeysockets/baileys';
import { Pool } from 'pg';
import { pino } from 'pino';

const logger = pino({ level: 'silent' });

export async function createDatabaseAuthState(
    pool: Pool,
    sessionId = 'default'
): Promise<{
    state: AuthenticationState;
    saveCreds: () => Promise<void>;
}> {

    await pool.query(`
        CREATE TABLE IF NOT EXISTS baileys_auth (
            session_id TEXT NOT NULL,
            key_type TEXT NOT NULL,
            key_id TEXT NOT NULL,
            value JSONB,
            PRIMARY KEY (session_id, key_type, key_id)
        )
    `);

    async function read(
        keyType: string,
        keyId: string
    ): Promise<any | null> {

        const result = await pool.query(
            `
            SELECT value
            FROM baileys_auth
            WHERE session_id = $1
              AND key_type = $2
              AND key_id = $3
            `,
            [
                sessionId,
                keyType,
                keyId
            ]
        );

        if (!result.rows.length) {
            return null;
        }

        return JSON.parse(
            JSON.stringify(
                result.rows[0].value
            ),
            BufferJSON.reviver
        );
    }

    async function write(
        keyType: string,
        keyId: string,
        value: any
    ) {

        if (value === null || value === undefined) {

            await pool.query(
                `
                DELETE FROM baileys_auth
                WHERE session_id = $1
                  AND key_type = $2
                  AND key_id = $3
                `,
                [
                    sessionId,
                    keyType,
                    keyId
                ]
            );

            return;
        }

        const serialized =
            JSON.parse(
                JSON.stringify(
                    value,
                    BufferJSON.replacer
                )
            );

        await pool.query(
            `
            INSERT INTO baileys_auth (
                session_id,
                key_type,
                key_id,
                value
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (
                session_id,
                key_type,
                key_id
            )
            DO UPDATE SET
                value = EXCLUDED.value
            `,
            [
                sessionId,
                keyType,
                keyId,
                serialized
            ]
        );
    }

    const storedCreds =
        await read(
            'creds',
            'creds'
        );

    const creds =
        storedCreds ||
        initAuthCreds();

    const keys: SignalKeyStore = {

        get: async <
            T extends keyof SignalDataTypeMap
        >(
            type: T,
            ids: string[]
        ) => {

            const result: {
                [id: string]:
                    SignalDataTypeMap[T]
            } = {};

            await Promise.all(
                ids.map(
                    async (id) => {

                        const value =
                            await read(
                                type,
                                id
                            );

                        if (
                            value !== null
                        ) {
                            result[id] =
                                value;
                        }
                    }
                )
            );

            return result;
        },

        set: async (
            data: SignalDataSet
        ) => {

            const operations:
                Promise<void>[] = [];

            for (
                const type of Object.keys(data)
            ) {

                const values =
                    data[
                        type as keyof SignalDataSet
                    ];

                if (!values) continue;

                for (
                    const id of Object.keys(values)
                ) {

                    const value =
                        values[id];

                    operations.push(
                        write(
                            type,
                            id,
                            value
                        )
                    );
                }
            }

            await Promise.all(
                operations
            );
        },

        clear: async () => {

            await pool.query(
                `
                DELETE FROM baileys_auth
                WHERE session_id = $1
                `,
                [sessionId]
            );
        }
    };

    const state: AuthenticationState = {
        creds,
        keys:
            makeCacheableSignalKeyStore(
                keys,
                logger
            )
    };

    const saveCreds = async () => {

        await write(
            'creds',
            'creds',
            state.creds
        );
    };

    return {
        state,
        saveCreds
    };
}