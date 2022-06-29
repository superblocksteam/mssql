import {
  Column,
  DatasourceMetadataDto,
  DBActionConfiguration,
  MsSqlDatasourceConfiguration,
  ExecutionOutput,
  IntegrationError,
  RawRequest,
  Table,
  TableType
} from '@superblocksteam/shared';
import {
  ActionConfigurationResolutionContext,
  BasePlugin,
  extractMustacheStrings,
  normalizeTableColumnNames,
  PluginExecutionProps,
  renderValue,
  resolveAllBindings
} from '@superblocksteam/shared-backend';
import { isEmpty } from 'lodash';
import mssql, { ConnectionPool } from 'mssql';

export default class MicrosoftSQLPlugin extends BasePlugin {
  pluginName = 'Microsoft SQL';
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async resolveActionConfigurationProperty(resolutionContext: ActionConfigurationResolutionContext): Promise<string | any[]> {
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
    return renderValue(propertyToResolve, bindingResolution);
  }

  async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<MsSqlDatasourceConfiguration>): Promise<ExecutionOutput> {
    let conn: ConnectionPool | undefined;
    try {
      conn = await this.createConnection(datasourceConfiguration);
      const query = actionConfiguration.body;

      const ret = new ExecutionOutput();
      if (isEmpty(query)) {
        return ret;
      }

      let request = conn.request();
      let paramCount = 1;
      for (const param of context.preparedStatementContext) {
        request = request.input(`PARAM_${paramCount++}`, param);
      }

      const result = await request.query(query);

      ret.output = normalizeTableColumnNames(result.recordset);

      return ret;
    } catch (err) {
      throw new IntegrationError(`${this.pluginName} query failed, ${err.message}`);
    } finally {
      if (conn) {
        conn.close();
      }
    }
  }

  getRequest(actionConfiguration: DBActionConfiguration): RawRequest {
    return actionConfiguration?.body;
  }

  dynamicProperties(): string[] {
    return ['body'];
  }

  private async createConnection(
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
      server: endpoint.host,
      port: Number(endpoint.port),
      requestTimeout: connectionTimeoutMillis,
      options: {
        trustServerCertificate: true,
        encrypt: datasourceConfiguration.connection?.useSsl ? true : false
      }
    };

    try {
      return await mssql.connect(sqlConfig);
    } catch (err) {
      throw new IntegrationError(`Failed to connect to ${this.pluginName}, ${err.message}`);
    }
  }

  async metadata(datasourceConfiguration: MsSqlDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    let conn: ConnectionPool | undefined;
    try {
      conn = await this.createConnection(datasourceConfiguration);

      const tables: Array<Table> = [];
      const resp = await mssql.query(
        "select table_name, column_name, data_type from information_schema.columns where table_schema != 'sys' order by ordinal_position"
      );
      const tablesMap: Record<string, Table> = {};

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
        conn.close();
      }
    }
  }

  async test(datasourceConfiguration: MsSqlDatasourceConfiguration): Promise<void> {
    let conn: ConnectionPool | undefined;
    try {
      conn = await this.createConnection(datasourceConfiguration);
      await mssql.query('select name from sys.databases');
    } catch (err) {
      throw new IntegrationError(`Test ${this.pluginName} connection failed, ${err.message}`);
    } finally {
      if (conn) {
        conn.close();
      }
    }
  }
}
