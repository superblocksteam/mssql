import {
  Column,
  DatasourceMetadataDto,
  DBActionConfiguration,
  MsSqlDatasourceConfiguration,
  ExecutionOutput,
  IntegrationError,
  RawRequest,
  Table,
  TableType,
  ResolvedActionConfigurationProperty
} from '@superblocksteam/shared';
import {
  ActionConfigurationResolutionContext,
  DatabasePluginPooled,
  extractMustacheStrings,
  normalizeTableColumnNames,
  PluginExecutionProps,
  renderValue,
  resolveAllBindings,
  CreateConnection,
  DestroyConnection,
  ResolveActionConfigurationProperty
} from '@superblocksteam/shared-backend';
import { isEmpty } from 'lodash';
import mssql, { ConnectionPool } from 'mssql';

export default class MicrosoftSQLPlugin extends DatabasePluginPooled<ConnectionPool, MsSqlDatasourceConfiguration> {
  pluginName = 'Microsoft SQL';
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  @ResolveActionConfigurationProperty
  public async resolveActionConfigurationProperty(
    resolutionContext: ActionConfigurationResolutionContext
  ): Promise<ResolvedActionConfigurationProperty> {
    if (!resolutionContext.actionConfiguration.usePreparedSql || resolutionContext.property !== 'body') {
      return super.resolveActionConfigurationProperty(resolutionContext);
    }
    const propertyToResolve = resolutionContext.actionConfiguration[resolutionContext.property] ?? '';
    const bindingResolution = {};
    const bindingResolutions = await resolveAllBindings(
      propertyToResolve,
      resolutionContext.context,
      resolutionContext.files ?? {},
      resolutionContext.escapeStrings
    );
    resolutionContext.context.preparedStatementContext = [];
    let bindingCount = 1;
    for (const toEval of extractMustacheStrings(propertyToResolve)) {
      bindingResolution[toEval] = `@PARAM_${bindingCount++}`;
      resolutionContext.context.preparedStatementContext.push(bindingResolutions[toEval]);
    }
    return { resolved: renderValue(propertyToResolve, bindingResolution) };
  }

  public init(): void {
    mssql.on('error', (err) => {
      this.logger.error(`${this.pluginName} connection errored: ${err}`);
    });
  }

  public async executePooled(
    { context, actionConfiguration }: PluginExecutionProps<MsSqlDatasourceConfiguration>,
    conn: ConnectionPool
  ): Promise<ExecutionOutput> {
    const query = actionConfiguration.body;
    const ret = new ExecutionOutput();
    if (!query || isEmpty(query)) {
      return ret;
    }
    let paramCount = 1;
    let result;
    try {
      result = await this.executeQuery(async () => {
        let request = conn.request();
        for (const param of context.preparedStatementContext) {
          request = request.input(`PARAM_${paramCount++}`, param);
        }
        return request.query(query);
      });
    } catch (err) {
      throw new IntegrationError(`${this.pluginName} query failed, ${err.message}`);
    }
    ret.output = normalizeTableColumnNames(result.recordset);
    return ret;
  }

  public getRequest(actionConfiguration: DBActionConfiguration): RawRequest {
    return actionConfiguration?.body;
  }

  public dynamicProperties(): string[] {
    return ['body'];
  }

  @DestroyConnection
  protected async destroyConnection(connection: ConnectionPool): Promise<void> {
    await connection.close();
  }

  @CreateConnection
  protected async createConnection(
    datasourceConfiguration: MsSqlDatasourceConfiguration,
    connectionTimeoutMillis = 30000
  ): Promise<ConnectionPool> {
    const endpoint = datasourceConfiguration.endpoint;
    const auth = datasourceConfiguration.authentication;
    if (!endpoint) {
      throw new IntegrationError(`Endpoint not specified for ${this.pluginName} step`);
    }
    if (!auth) {
      throw new IntegrationError(`Authentication not specified for ${this.pluginName} step`);
    }
    if (!auth.custom?.databaseName?.value) {
      throw new IntegrationError(`Database not specified for ${this.pluginName} step`);
    }

    const sqlConfig = {
      user: auth.username,
      password: auth.password,
      database: auth.custom.databaseName.value,
      server: endpoint.host ?? '',
      port: Number(endpoint.port),
      requestTimeout: connectionTimeoutMillis,
      options: {
        trustServerCertificate: true,
        encrypt: datasourceConfiguration.connection?.useSsl ? true : false
      }
    };

    try {
      const connPool = new mssql.ConnectionPool(sqlConfig);
      await connPool.connect();
      return connPool;
    } catch (err) {
      throw new IntegrationError(`Failed to connect to ${this.pluginName}, ${err.message}`);
    }
  }

  public async metadata(datasourceConfiguration: MsSqlDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    const conn = await this.createConnection(datasourceConfiguration);
    const tables: Array<Table> = [];
    const tablesMap: Record<string, Table> = {};
    let resp;
    try {
      resp = await this.executeQuery(() => {
        return conn.query(
          "select table_name, column_name, data_type from information_schema.columns where table_schema != 'sys' order by ordinal_position"
        );
      });

      for (const record of resp.recordset) {
        if (tablesMap[record.table_name]) {
          tablesMap[record.table_name].columns.push(new Column(record.column_name, record.data_type));
        } else {
          const table = new Table(record.table_name, TableType.TABLE);
          table.columns.push(new Column(record.column_name, record.data_type));
          tablesMap[record.table_name] = table;
        }
      }

      for (const table of Object.values(tablesMap)) {
        tables.push(table);
      }

      return {
        dbSchema: { tables: tables }
      };
    } catch (err) {
      throw new IntegrationError(`Failed to connect to ${this.pluginName}, ${err.message}`);
    } finally {
      if (conn) {
        this.destroyConnection(conn).catch(() => {
          // Error handling is done in the decorator
        });
      }
    }
  }

  public async test(datasourceConfiguration: MsSqlDatasourceConfiguration): Promise<void> {
    const conn = await this.createConnection(datasourceConfiguration);
    try {
      await this.executeQuery(() => {
        return conn.query('select name from sys.databases');
      });
    } catch (err) {
      throw new IntegrationError(`Test ${this.pluginName} connection failed, ${err.message}`);
    } finally {
      if (conn) {
        this.destroyConnection(conn).catch(() => {
          // Error handling is done in the decorator
        });
      }
    }
  }
}
