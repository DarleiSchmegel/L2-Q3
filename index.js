const sqlite3 = require("sqlite3").verbose();

// let db = new sqlite3.Database(":memory:");

let db = new sqlite3.Database("cadastro.sqlite", (err) => {
  if (err) {
    return console.error(err.message);
  }
  console.log("Connected to the cadastro.sqlite SQlite database.");
  main();
});

const contratos = [
  { ID: 1, VALOR_PARCELA: 150, PARCELAS: 100 },
  { ID: 2, VALOR_PARCELA: 300, PARCELAS: 48 },
  { ID: 3, VALOR_PARCELA: 550, PARCELAS: 24 },
  { ID: 4, VALOR_PARCELA: 1000, PARCELAS: 12 },
];

const pessoas = [
  {
    ID: 1,
    NOME: "Cristian Ghyprievy",
    CONTRATO_ID: 2,
    INADIMPLENTE: "S",
    DT_COMPLETO: "NULL",
  },
  {
    ID: 2,
    NOME: "Joana Cabel",
    CONTRATO_ID: 1,
    INADIMPLENTE: "S",
    DT_COMPLETO: "NULL",
  },
  {
    ID: 3,
    NOME: "John Serial",
    CONTRATO_ID: 3,
    INADIMPLENTE: "S",
    DT_COMPLETO: "NULL",
  },
  {
    ID: 4,
    NOME: "Michael Seven",
    CONTRATO_ID: 2,
    INADIMPLENTE: "N",
    DT_COMPLETO: "2021-09-25",
  },
];

const pagamentos = [
  { ID: 1, PESSOA_ID: "4", DT_PAGAMENTO: "2021-09-01" },
  { ID: 2, PESSOA_ID: "3", DT_PAGAMENTO: "2021-09-05" },
  { ID: 3, PESSOA_ID: "1", DT_PAGAMENTO: "2021-09-19" },
  { ID: 4, PESSOA_ID: "2", DT_PAGAMENTO: "2021-09-25" },
];

const query = `
CREATE TABLE CONTRATOS (  ID serial PRIMARY KEY, 
                          VALOR_PARCELA INTEGER NOT NULL, 
                          PARCELAS INTEGER NOT NULL);                        
INSERT INTO CONTRATOS ( VALOR_PARCELA, PARCELAS) VALUES ('150', '100'),
                      ( VALOR_PARCELA, PARCELAS) VALUES ('300', '48'),
                      ( VALOR_PARCELA, PARCELAS) VALUES ('550', '24'),
                      ( VALOR_PARCELA, PARCELAS) VALUES ('1000', '12');

CREATE TABLE PESSOAS (  ID serial PRIMARY KEY, 
                        NOME VARCHAR(100) NOT NULL, 
                        CONTRATO_ID INTEGER NOT NULL, 
                        INADIMPLENTE VARCHAR(1) NOT NULL,
                        DT_COMPLETO DATE, 
                        FOREIGN KEY (CONTRATO_ID) REFERENCES CONTRATOS (ID)
                        ON DELETE CASCADE);
INSERT INTO PESSOAS ( NOME, CONTRATO_ID, INADIMPLENTE, DT_COMPLETO) VALUES ('Cristian Ghyprievy', '2', 'S', 'NULL'),
                    ( NOME, CONTRATO_ID, INADIMPLENTE, DT_COMPLETO) VALUES ('Joana Cabel', '1', 'S', 'NULL'),
                    ( NOME, CONTRATO_ID, INADIMPLENTE, DT_COMPLETO) VALUES ('John Serial', '3', 'S', 'NULL'),
                    ( NOME, CONTRATO_ID, INADIMPLENTE, DT_COMPLETO) VALUES ('Michael Seven', '2', 'N', '2021-09-25');

CREATE TABLE PAGAMENTOS ( ID serial PRIMARY KEY, 
                          PESSOA_ID INTEGER NOT NULL, 
                          DT_PAGAMENTO DATE NOT NULL
                          FOREIGN KEY (PESSOA_ID) 
                          REFERENCES PESSOAS (ID)  ON DELETE CASCADE);

  INSERT INTO PAGAMENTOS ( PESSOA_ID, DT_PAGAMENTO) VALUES ('4', '2021-09-01'),
                         ( PESSOA_ID, DT_PAGAMENTO) VALUES ('3', '2021-09-01'),
                         ( PESSOA_ID, DT_PAGAMENTO) VALUES ('1', '2021-09-01'),
                         ( PESSOA_ID, DT_PAGAMENTO) VALUES ('2', '2021-09-01'),


`;

async function main() {
  db.serialize(() => {
    //Criando e inserindo na tabela de Contratos
    db.run("DROP TABLE IF EXISTS CONTRATOS");

    db.run(`CREATE TABLE if not exists CONTRATOS (ID INTEGER PRIMARY KEY AUTOINCREMENT, 
      VALOR_PARCELA INTEGER NOT NULL, 
      PARCELAS INTEGER NOT NULL)`);
    const stmt_contratos = db.prepare(
      `INSERT INTO CONTRATOS ( VALOR_PARCELA, PARCELAS) VALUES (?, ?)`
    );

    contratos.map((contato) => {
      stmt_contratos.run(contato.VALOR_PARCELA, contato.PARCELAS);
    });

    stmt_contratos.finalize();

    db.each(`SELECT * FROM CONTRATOS;`, (err, row) => {
      if (err) {
        console.error(err.message);
      }
      console.log(
        row.ID + "\t" + "\t" + row.VALOR_PARCELA + "\t" + row.PARCELAS
      );
    });

    //Criando e inserindo na tabela de pessoas
    db.run("DROP TABLE IF EXISTS PESSOAS");

    db.run(`CREATE TABLE if not exists PESSOAS ( 
            ID INTEGER PRIMARY KEY AUTOINCREMENT, 
            NOME VARCHAR(100) NOT NULL, 
            CONTRATO_ID INTEGER NOT NULL, 
            INADIMPLENTE VARCHAR(1) NOT NULL,
            DT_COMPLETO DATE, 
            FOREIGN KEY (CONTRATO_ID) REFERENCES CONTRATOS (ID)
            ON DELETE CASCADE);`);
    const stmt_pessoas = db.prepare(
      `INSERT INTO PESSOAS ( NOME, CONTRATO_ID, INADIMPLENTE, DT_COMPLETO) VALUES (?, ?, ?, ?);`
    );

    pessoas.map((pessoa) => {
      stmt_pessoas.run(
        pessoa.NOME,
        pessoa.CONTRATO_ID,
        pessoa.INADIMPLENTE,
        pessoa.DT_COMPLETO
      );
    });

    stmt_pessoas.finalize();

    // console.log("TABELA PESSOAS");
    db.each(`SELECT * FROM PESSOAS;`, (err, row) => {
      if (err) {
        console.error(err.message);
      }
      console.log(
        row.ID +
          "\t" +
          row.NOME +
          "\t" +
          row.CONTRATO_ID +
          "\t" +
          row.INADIMPLENTE +
          "\t" +
          row.DT_COMPLETO
      );
    });

    //Criando e inserindo na Tabela Pagamentos
    db.run("DROP TABLE IF EXISTS PAGAMENTOS");

    db.run(`CREATE TABLE if not exists PAGAMENTOS ( 
      ID INTEGER PRIMARY KEY AUTOINCREMENT,
      PESSOA_ID INTEGER NOT NULL, 
      DT_PAGAMENTO DATE NOT NULL,
      FOREIGN KEY (PESSOA_ID) 
      REFERENCES PESSOAS (ID)  ON DELETE CASCADE)`);
    const stmt_pagamentos = db.prepare(
      `INSERT INTO PAGAMENTOS ( PESSOA_ID, DT_PAGAMENTO) VALUES (?, ?)`
    );

    pagamentos.map((pagamento) => {
      stmt_pagamentos.run(pagamento.PESSOA_ID, pagamento.DT_PAGAMENTO);
    });

    stmt_pagamentos.finalize();

    db.each(`SELECT * FROM PAGAMENTOS`, (err, row) => {
      if (err) {
        console.error(err.message);
      }
      console.log(row.ID + "\t" + row.PESSOA_ID + "\t" + row.DT_PAGAMENTO);
    });

    db.each(`SELECT * FROM PAGAMENTOS`, (err, row) => {
      if (err) {
        console.error(err.message);
      }
      console.log(row.ID + "\t" + row.PESSOA_ID + "\t" + row.DT_PAGAMENTO);
    });

    //Resultado Esperado - Inadimplentes
    db.run("DROP TABLE IF EXISTS INADIMPLENTES");
    db.each(
      `
      CREATE TABLE INADIMPLENTES AS
      SELECT 
              PESSOAS.NOME AS NOME,
              strftime('%d', PAGAMENTOS.DT_PAGAMENTO) AS "DIA_MES",
              CONTRATOS.VALOR_PARCELA AS VALOR_PARCELA
      FROM
            PESSOAS 
            INNER JOIN CONTRATOS ON CONTRATOS.ID = PESSOAS.CONTRATO_ID AND PESSOAS.INADIMPLENTE = "S"  
            INNER JOIN PAGAMENTOS ON PAGAMENTOS.PESSOA_ID = PESSOAS.ID
      ORDER BY NOME
          ;`,
      (err, row) => {
        if (err) {
          console.error(err.message);
        }
        console.log(row);
      }
    );

    //Resultado Esperado - Pagamento completo
    db.run("DROP TABLE IF EXISTS PAGAMENTO_COMPLENTO");
    db.each(
      `
      
      CREATE TABLE PAGAMENTO_COMPLENTO AS
      SELECT 
              PESSOAS.NOME AS NOME,
              CONTRATOS.VALOR_PARCELA * PARCELAS AS VALOR_TOTAL
      FROM
            PESSOAS 
            INNER JOIN CONTRATOS ON CONTRATOS.ID = PESSOAS.CONTRATO_ID AND PESSOAS.DT_COMPLETO 
      ORDER BY NOME 
          ;`,
      (err, row) => {
        if (err) {
          console.error(err.message);
        }
        console.log("RETORNO", row);
      }
    );
  });

  db.close();
}
