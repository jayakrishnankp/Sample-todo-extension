class SQLStore {
  static db = null;
  constructor(options) {
    this.tableName = options.name;
  }

  get(callback) {
    let sqlQry = `SELECT * FROM ${this.tableName}`;
    execQuery(sqlQry, (result) => {
      callback(result);
    });
  }

  insert(doc) {
    console.log("Insert", doc.title, doc.content);
    let sqlQry = `INSERT INTO ${this.tableName}(id, title, content) VALUES ('${Date.now()}', "${doc.title}", "${doc.content}")`;
    execQuery(sqlQry);
  }

  delete(doc) {
    console.log("Deleting ", doc.id)
    let sqlQry = `DELETE FROM ${this.tableName} where id=${doc.id}`;
    execQuery(sqlQry);
  }
}

function tableCreate(store) {
  let sqlQry = `CREATE TABLE IF NOT EXISTS ${store.tableName}(id TEXT PRIMARY KEY, title TEXT, content TEXT)`;
  execQuery(sqlQry);
}

function execQuery(sql, callback) {
  SQLStore.db.transaction( tx => {
    tx.executeSql(sql, [], (tx, rs) => {
      if(sql.split(' ')[0] === "SELECT"){
        callback(rs.rows);
      } else {
        console.log("Completed transaction");
      }
    })
  })
}

function initStores() {
  tableCreate(taskStore);
  tableCreate(sampleStore);
}

function openConnection(name) {
  SQLStore.db = openDatabase(name, 1, 'Dummy data store', 1000 * 1024 * 1024);
  window.taskStore = new SQLStore({
    name: "tasks"
  })
  window.sampleStore = new SQLStore({
    name: "sample"
  })
  initStores();
}

openConnection("DummyDB");