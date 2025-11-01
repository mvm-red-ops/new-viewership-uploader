const snowflake = require("snowflake-sdk");
// const dotenv = require("dotenv");

let globalConnection = null;

class SnowFlakeMethods {
    constructor() {
        this.conn = null;
    }

    async connect() {
        if (this.conn) {
            console.log("Connection already established.");
            return this.conn;
        }
        if (!globalConnection) {
            console.log("Initializing new connection...");
            try {
                const conn = snowflake.createConnection({
                    account: process.env.SNOWFLAKE_ACCOUNT,
                    username: process.env.SNOWFLAKE_USERNAME,
                    password: process.env.SNOWFLAKE_PASSWORD,
                    role: process.env.SNOWFLAKE_ROLE,
                    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
                    database: process.env.SNOWFLAKE_UPLOADER_DATABASE,
                    schema: process.env.SNOWFLAKE_SCHEMA,
                });

                await new Promise((resolve, reject) => {
                    conn.connect((err, conn) => {
                        if (err) {
                            console.error("Failed to connect to Snowflake", err);
                            reject(err);
                        } else {
                            console.log("Connected to Snowflake");
                            resolve(conn);
                        }
                    });
                });

                this.conn = conn;
                globalConnection = conn;
            } catch (error) {
                console.error("Error connecting to Snowflake:", error);
                throw error;
            }
        } else {
            console.log("Reusing existing connection.");
            this.conn = globalConnection;
        }
        return this.conn;
    }

    destroy() {
        if (this.conn) {
            this.conn.destroy(() => {
                console.log("Destroyed connection");
            });
            this.conn = null;
            globalConnection = null;
        }
    }
}

const executeQueryWithoutBinds = async (query) => {
    return new Promise(async (resolve, reject) => {
        const snowflakeMethods = new SnowFlakeMethods();
        try {
            const conn = await snowflakeMethods.connect();
            console.log("Connected to Snowflake for query without binds. Query: ", query);
            
            conn.execute({
                sqlText: query,
                complete: function (err, stmt, rows) {
                    if (err) {
                        console.error("Error in running query", err);
                        reject({ err });
                    } else {
                        console.log("Query executed successfully without binds.");
                        resolve(rows);
                    }
                }
            });
        } catch (connectionError) {
            console.error("Error establishing connection with Snowflake:", connectionError);
            reject({ err: connectionError });
        }
    });
};

module.exports = { SnowFlakeMethods, executeQueryWithoutBinds };