const simpleGit = require("simple-git");
const oracledb = require("oracledb");
oracledb.fetchAsString = [oracledb.CLOB];
const fs = require("fs");
const path = require("path");

const watch = require("./watch");

const REMOTE_PATH = "https://github.com/subutuo/CCNB_PCK_LOG.git";

let connection;
let git;

const watchItems = Object.keys(watch).reduce((acc, key) => acc.concat(watch[key]), []);
const procNames = watchItems.map((i) => i.name);

/// 개발
const server = {
  user: "cisadm",
  password: "KDcbAdm063",
  connectString: "10.77.10.152:1522/CISDEV"
};

const {CronJob} = require("cron");

(async () => {
  await gitInit();
  var job = new CronJob(
    "*/10 * * * * *",
    async function () {
      console.log("cron 실행");
      await detectChanges(server);
    },
    null,
    true,
    "America/Los_Angeles"
  );
  job.start();
})();

// Git Clone Or Pull 진행
async function gitInit() {
  git = simpleGit();

  const fileList = fs.readdirSync(".");
  if (!fileList.includes('.git')) {
    git.init().addRemote('origin', REMOTE_PATH);
  }
}

async function detectChanges(server) {
  const {user, password, connectString} = server;
  connection = await oracledb.getConnection({
    user,
    password,
    connectString
  });
  // console.log("Successfully connected to Oracle!");

  const sql = `
    SELECT t.object_name
        , TO_CHAR(t.last_ddl_time, 'yyyymmddhh24miss') AS last_ddl_time
      FROM user_objects t
      WHERE 1 = 1
      AND t.object_type LIKE 'PACKAGE'
      AND t.object_name IN (${procNames.map((name) => `'${name}'`).join(", ")})
  `;

  try {
    let result = await connection.execute(sql, [], {outFormat: oracledb.OUT_FORMAT_OBJECT});

    const data = result.rows;

    if (!data || data.length != watchItems.length) {
      throw new Error("인출 행 상이함");
    }

    for (const [_, item] of watchItems.entries()) {
      const {name} = item;

      const dbData = data.filter((i) => i["OBJECT_NAME"] == name)[0];

      const newProcName = `${name}_${dbData["LAST_DDL_TIME"]}.pck`;
      const newDdlTime = dbData["LAST_DDL_TIME"];

      const source = await getPackageSource(name);

      fs.writeFileSync(path.join("git", name), source, {
        encoding: "utf-8"
      });

      git.add(".", () => {
        console.log('add함', arguments)
      }).commit(`commit - ${newProcName}`).push("origin", "master", ['--force']);

      // 신규 watch 또는 패키지 변경일 경우 메모리에 저장
      item.lastProcName = newProcName;
      item.lastDdlTime = newDdlTime;
    }
  } catch (err) {
    console.log("Error: ", err);
    if (connection) {
      await connection.close();
      return;
    }
  } finally {
    if (connection) {
      await connection.close();
    }
  }
}

async function getPackageSource(procName) {
  const sql = `
    SELECT dbms_metadata.get_ddl('PACKAGE', '${procName}') AS SOURCE -- PACKAGE_BODY 내용도 같이 보여줌
    FROM dual
  `;

  try {
    let result = await connection.execute(sql, [], {outFormat: oracledb.OUT_FORMAT_OBJECT});

    const data = result.rows;

    if (data.length) {
      return data[0].SOURCE;
    } else {
      throw new Error("소스 받기 실패");
    }
  } catch (e) {}
}
