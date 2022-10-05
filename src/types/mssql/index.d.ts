import { ConnectionPool } from 'mssql';

export declare function on(event: string, handler: (any) => void): ConnectionPool;
