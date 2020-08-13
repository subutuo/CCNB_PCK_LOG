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
  init();
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

  // console.log("git pull 실행");
  // return new Promise((resolve, reject) => {
  //   git
  //     .pull("origin", "master")
  //     .then(() => {
  //       console.log("git pull finished");
  //       resolve();
  //     })
  //     .catch((err) => {
  //       console.error("git pull failed: ", err);
  //       reject();
  //     });
  // });
  
}

function init() {
  const savedFileList = fs.readdirSync("./backup");

  for (const [_, item] of watchItems.entries()) {
    const name = item.name;

    const existNames = savedFileList.filter((savedFile) => {
      return new RegExp(`^${name}`).test(savedFile);
    });

    existNames.sort((a, b) => b.match(/\d{14}/)[0] - a.match(/\d{14}/)[0]);

    item.lastProcName = existNames.length ? existNames[0] : "";
    item.lastDdlTime = existNames.length ? existNames[0].match(/\d{14}/)[0] : "";
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

    // lastDdlTime 같은지 체크
    for (const [_, item] of watchItems.entries()) {
      const {name, lastDdlTime} = item;

      const dbData = data.filter((i) => i["OBJECT_NAME"] == name)[0];
      // console.log(lastDdlTime, dbData["LAST_DDL_TIME"])

      // 변동없음을 체크
      if (lastDdlTime == dbData["LAST_DDL_TIME"]) {
        continue;
      }

      if (lastDdlTime) {
        console.log("패키지 변경", name);
      } else {
        console.log("패키지 신규", name);
      }

      const newProcName = `${name}_${dbData["LAST_DDL_TIME"]}.pck`;
      const newDdlTime = dbData["LAST_DDL_TIME"];

      const source = await getPackageSource(name);

      fs.writeFileSync(path.join("git", name), source, {
        encoding: "utf-8"
      });
      fs.writeFileSync(path.join("backup", newProcName), source, {
        encoding: "utf-8"
      });

      console.log("저장완료", newProcName);

      git.add(".").commit(`commit - ${newProcName}`).push("origin", "master", '--force');

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
