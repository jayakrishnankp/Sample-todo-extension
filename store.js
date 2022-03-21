class SQLStore {
  static db = null;
  constructor(options) {
    this.tableName = options.name;
  }

  get(callBack) {
    SQLStore.db.transaction((tx) => {
      tx.executeSql(`SELECT * FROM ${this.tableName}`, [], function (tx, rs) {
        callBack(rs.rows);
      });
    });
  }

  insert(doc) {
    console.log("Insert", doc.title, doc.content);
    SQLStore.db.transaction((tx) => {
      tx.executeSql(
        `INSERT INTO ${this.tableName}(id, title, content) VALUES ('${Date.now()}', "${doc.title}", "${doc.content}")`,
        [],
        function (tx) { console.log("Insert", tx) }
      );
    })
  }

  delete(doc) {
    console.log("Deleting ", doc.id)
    SQLStore.db.transaction((tx) => {
      tx.executeSql(
        `DELETE FROM ${this.tableName} where id=${doc.id}`,
        [],
        function (tx) { console.log("Delete", tx) });
    })
  }
}

function tableCreate(store) {

  SQLStore.db.transaction(function (tx) {
    console.log("Create Table")
    tx.executeSql(
      `CREATE TABLE IF NOT EXISTS ${store.tableName}(id TEXT PRIMARY KEY, title TEXT, content TEXT)`,
      [],
      function (tx) { console.log(tx) }, null);
  });
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