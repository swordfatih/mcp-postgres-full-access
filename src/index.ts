#!/usr/bin/env node
import express from "express";
import { randomUUID } from "node:crypto";
import { config as dotenvConfig } from "dotenv";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import pg from "pg";

// Load env variables
dotenvConfig();

import config from "./lib/config.js";
import { TransactionManager } from "./lib/transaction-manager.js";
import { safelyReleaseClient } from "./lib/utils.js";
import { handleExecuteQuery, handleExecuteDML, handleExecuteCommit, handleExecuteMaintenance, handleListTables, handleDescribeTable, handleListResources, handleReadResource } from "./lib/tool-handlers.js";

const PORT = parseInt(process.env.PORT || "8080", 10);
const DATABASE_URL = process.argv[2];

if (!DATABASE_URL) {
    console.error("❌ Please provide a database URL as a command-line argument");
    process.exit(1);
}

const resourceBaseUrl = new URL(DATABASE_URL);
resourceBaseUrl.protocol = "postgres:";
resourceBaseUrl.password = "";

const pool = new pg.Pool({
    connectionString: DATABASE_URL,
    max: config.pg.maxConnections,
    idleTimeoutMillis: config.pg.idleTimeoutMillis,
    statement_timeout: config.pg.statementTimeout,
});

const transactionManager = new TransactionManager(config.transactionTimeoutMs, config.monitorIntervalMs, config.enableTransactionMonitor);

function transformHandlerResponse(result: any) {
    if (!result) return result;
    const transformedResult = { ...result };
    if (result.content) {
        transformedResult.content = result.content.map((item: any) => {
            if (item.type === "text") {
                return { type: "text" as const, text: item.text };
            }
            return item;
        });
    }
    return transformedResult;
}

function createServer(): McpServer {
    const server = new McpServer(
        {
            name: "postgres-advanced",
            version: "0.3.1",
        },
        {
            capabilities: {
                resources: {},
                tools: {},
            },
        }
    );

    // Tools
    server.tool("execute_query", "Run a read-only SQL query", { sql: z.string() }, async ({ sql }) => {
        try {
            return transformHandlerResponse(await handleExecuteQuery(pool, sql));
        } catch (err) {
            return { isError: true, content: [{ type: "text", text: (err as Error).message }] };
        }
    });

    server.tool("execute_dml_ddl_dcl_tcl", "Run modifying SQL", { sql: z.string() }, async ({ sql }) => {
        try {
            if (transactionManager.transactionCount >= config.maxConcurrentTransactions) {
                return {
                    isError: true,
                    content: [{ type: "text", text: `Limit reached (${config.maxConcurrentTransactions}). Try later.` }],
                };
            }
            return transformHandlerResponse(await handleExecuteDML(pool, transactionManager, sql, config.transactionTimeoutMs));
        } catch (err) {
            return { isError: true, content: [{ type: "text", text: (err as Error).message }] };
        }
    });

    server.tool("execute_commit", "Commit a transaction", { transaction_id: z.string() }, async ({ transaction_id }) => {
        try {
            return transformHandlerResponse(await handleExecuteCommit(transactionManager, transaction_id));
        } catch (err) {
            return { isError: true, content: [{ type: "text", text: (err as Error).message }] };
        }
    });

    server.tool("execute_rollback", "Rollback a transaction", { transaction_id: z.string() }, async ({ transaction_id }) => {
        try {
            if (!transactionManager.hasTransaction(transaction_id)) {
                return { isError: true, content: [{ type: "text", text: `Transaction not found: ${transaction_id}` }] };
            }
            const tx = transactionManager.getTransaction(transaction_id)!;
            if (tx.released) {
                transactionManager.removeTransaction(transaction_id);
                return { isError: true, content: [{ type: "text", text: `Already released: ${transaction_id}` }] };
            }
            await tx.client.query("ROLLBACK");
            tx.released = true;
            safelyReleaseClient(tx.client);
            transactionManager.removeTransaction(transaction_id);
            return {
                content: [{ type: "text", text: `Rolled back: ${transaction_id}. No changes applied.` }],
            };
        } catch (err) {
            return { isError: true, content: [{ type: "text", text: (err as Error).message }] };
        }
    });

    server.tool("execute_maintenance", "Run VACUUM, ANALYZE, etc.", { sql: z.string() }, async ({ sql }) => {
        try {
            return transformHandlerResponse(await handleExecuteMaintenance(pool, sql));
        } catch (err) {
            return { isError: true, content: [{ type: "text", text: (err as Error).message }] };
        }
    });

    server.tool("list_tables", "List tables", { schema_name: z.string() }, async ({ schema_name }) => {
        try {
            return transformHandlerResponse(await handleListTables(pool, schema_name));
        } catch (err) {
            return { isError: true, content: [{ type: "text", text: (err as Error).message }] };
        }
    });

    server.tool("describe_table", "Describe a table", { table_name: z.string(), schema_name: z.string().default("public") }, async ({ table_name, schema_name }) => {
        try {
            return transformHandlerResponse(await handleDescribeTable(pool, table_name, schema_name));
        } catch (err) {
            return { isError: true, content: [{ type: "text", text: (err as Error).message }] };
        }
    });

    const schemaTemplate = new URL(`{tableName}/schema`, resourceBaseUrl);

    server.resource("database-schemas", resourceBaseUrl.href, { description: "List schemas" }, async (uri) => {
        const result = await handleListResources(pool, resourceBaseUrl);
        return {
            contents: [
                {
                    uri: uri.href,
                    mimeType: "application/json",
                    text: JSON.stringify(result.resources, null, 2),
                },
            ],
        };
    });

    server.resource("table-schemas", schemaTemplate.href, { description: "Table schemas" }, async (uri) => {
        return await handleReadResource(pool, uri.href);
    });

    return server;
}

const transports: { [sessionId: string]: StreamableHTTPServerTransport } = {};

const app = express();
app.use(express.json());

app.post("/mcp", async (req, res) => {
    console.log("\n🔹 Incoming /mcp POST request");
    console.log("🔸 Headers:", req.headers);
    console.log("🔸 Body:", JSON.stringify(req.body, null, 2));

    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    console.log("🔸 Session ID from header:", sessionId ?? "none");

    let transport: StreamableHTTPServerTransport;

    const isInitRequest = req.body?.method === "initialize" && typeof req.body?.params === "object" && typeof req.body?.params?.protocolVersion === "string";
    console.log("🔍 isInitializeRequest manual check:", isInitRequest);

    if (sessionId && transports[sessionId]) {
        console.log("✅ Reusing existing session:", sessionId);
        transport = transports[sessionId];
        console.log("✅ Request handled by existing session");
    } else if (!sessionId && isInitRequest) {
        console.log("🆕 No session ID provided; request looks like initialization");

        const newSessionId = randomUUID();
        res.setHeader("mcp-session-id", newSessionId); // 🔐 Set header before sending response

        transport = new StreamableHTTPServerTransport({
            sessionIdGenerator: () => newSessionId,
            onsessioninitialized: (id) => {
                console.log("✅ onsessioninitialized called — session active:", id);
                transports[id] = transport;
            },
        });

        transport.onclose = () => {
            if (transport.sessionId) {
                console.log("🛑 Session closed and cleaned up:", transport.sessionId);
                delete transports[transport.sessionId];
            }
        };

        const server = createServer();
        console.log("🔧 Created new MCP server instance");
        await server.connect(transport);
        console.log("🔗 Server connected to transport");
    } else {
        // Invalid request
        res.status(400).json({
            jsonrpc: "2.0",
            error: {
                code: -32000,
                message: "Bad Request: No valid session ID provided",
            },
            id: null,
        });
        return;
    }

    await transport.handleRequest(req, res, req.body);
    console.log("📤 Request handled after session setup !");
});

const handleSessionRequest = async (req: express.Request, res: express.Response) => {
    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    if (!sessionId || !transports[sessionId]) {
        res.status(400).send("Invalid or missing session ID");
        return;
    }

    const transport = transports[sessionId];
    await transport.handleRequest(req, res);
};

app.get("/mcp", handleSessionRequest);
app.delete("/mcp", handleSessionRequest);

app.listen(PORT, () => {
    console.log(`✅ MCP PostgreSQL server listening on http://localhost:${PORT}/mcp`);
});
