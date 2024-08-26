const odbc = require("odbc");
const sqlKeywords = new Set([
  "SELECT", "FROM", "WHERE", "ORDER", "BY", "DESC", "ASC", "LIMIT",
  "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
  "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "ON",
  "GROUP", "HAVING", "DISTINCT", "UNION", "ALL",
  "AS", "IN", "AND", "OR", "NOT", "NULL", "IS",
  "BETWEEN", "LIKE", "ILIKE", "IN", "EXISTS",
  "CREATE", "TABLE", "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
  "ALTER", "ADD", "DROP", "CONSTRAINT", "DEFAULT",
  "DATABASE", "INDEX", "VIEW", "TRIGGER", "PROCEDURE", "FUNCTION",
  "GRANT", "REVOKE", "WITH", "CHECK", "IF", "CASE", "WHEN", "THEN", "ELSE", "END",
  "*", "SHOW", "COLUMNS", "=", "!=", ">", "<", ">=", "<="
  // Ajouter plus de mots-clés SQL si nécessaire
]);

class ODBCConnector {
  #connected = false;
  database_id = "";
  constructor(
    config = {
      connectionString: null,
    }
  ) {
    this.connectionString = config.connectionString;
    this._client = null;
    this.database_id = this.#parseDatabase();
  }

  #parseDatabase() {
    const regex = /Database=([^;]+)/;
    const match = this.connectionString.match(regex);
    return match ? match[1] : null;
  }

  async connect() {
    this._client = await odbc.connect(this.connectionString);
    this.#connected = true;
    return this._client;
  }

  /**
   * add backtick automaticaly
   * @param {string} queryString the SQL query to be run
   * @returns {string} transformedQueryString
   */
  addBackticks(queryString) {
    let mysplit = queryString.split(" ");
    let result = "";
    if (queryString.includes("information_schema") || queryString.includes("SHOW COLUMNS FROM")) {
      return queryString;
    }
    mysplit.forEach((element) => {
      //console.log(element);
      // Ignorer les mots-clés SQL
      if (!sqlKeywords.has(element.toUpperCase()) && !element.match(/^\d+$/)) {
        let parts = element.split("=");
        if (parts.length === 1) {
          // si contient un point ou commence par un chiffre
          if (
            (element.match(/^\d/) || element.match(/\./)) &&
            !element.match(/;/)
          ) {
            result += `\`${element}\` `;
          } else {
            result += `${element} `;
          }
        } else {
          // Ajoute des backticks à la partie avant le signe égal
          result += `\`${parts[0]}\`=${parts[1]} `;
        }
      } else {
        result += element + " ";
      }
    });
    return result.trim();
  }

  /**
   * convertBigIntsInObject
   * @param  obj to convert
   * @returns  obj converted
   */
  convertBigIntsInObject(obj) {
    if (typeof obj !== "object" || obj === null) {
      // Si ce n'est pas un objet, on retourne la valeur telle quelle
      return obj;
    }

    // Parcourir les clés de l'objet
    for (const key in obj) {
      if (typeof obj[key] === "bigint") {
        // Convertir BigInt en Number
        obj[key] = Number(obj[key]);
      } else if (typeof obj[key] === "object") {
        // Appel récursif si l'élément est un objet ou un tableau
        this.convertBigIntsInObject(obj[key]);
      }
    }
    return obj;
  }

  /**
   *
   * @param {string} queryString the SQL query to be run
   * @returns {import(".").QueryResult}
   */
  async runQuery(queryString = "") {
    const result = { rows: [], count: 0, error: null };
    try {
      if (!this.#connected) await this.connect();
      const transformedQueryString = this.addBackticks(queryString);
      console.log(this.constructor.name, "request with backtick",transformedQueryString);
      const query = await this._client.query(transformedQueryString);
      result.rows = this.convertBigIntsInObject(query);
      result.count = query.length;
    } catch (err) {
      console.log(this.constructor.name, err);
      result.error = err.message;
    } finally {
      try {   // can have a problem with mongoDB odbc driver (linux)
        // [MySQL][ODBC 1.4(a) Driver]Underlying server does not support transactions, upgrade to version >= 3.23.38
        await this._client.close();
      } catch (closeErr) {
        console.log(this.constructor.name, "Error closing client (linux?):", closeErr);
        // Optionally, you could add this to the result object
        // result.error = result.error ? result.error + '; ' + closeErr.message : closeErr.message;
      }
      this.#connected = false;
    }
    return result;
  }

  getTablesSql() {
    return `SELECT table_name FROM information_schema.tables WHERE table_schema = '${this.database_id}'`;
  }

  getTableSchemaSql(table_name) {
    return `SHOW COLUMNS FROM ${this.database_id}.\`${table_name}\`;`;
  }
}

module.exports.ODBCConnector = ODBCConnector;
