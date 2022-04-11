// Download / Update BotW runs from the speedrun.com API

var fs = require('fs');
import fetch from 'isomorphic-unfetch'

type obj = { [key: string]: any };


function write_json(file: string, data: any) {
  fs.writeFileSync(file, JSON.stringify(data));
}
function read_json(file: string): any {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    console.log("Error reading file: ", file);
    return [];
  }
}
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const api = "https://www.speedrun.com/api/v1"

function runs_url(gameid: string, offset: number, maxn: number): string {
  return `${api}/runs?game=${gameid}&offset=${offset}&max=${maxn}&orderby=submitted&direction=asc&embed=players`;
}

async function get_json(url: string): Promise<any> {
  return fetch(url)
    .then((res: any) => {
      if (res.status == 404) {
        console.log(`   File not found: ${url}`);
        return {};
      }
      if (res.status >= 400 && res.status < 600) {
        throw new Error("Status Error: " + res.status);
      }
      return res.json();
    }).catch(function(error: any) {
      console.log(error);
      process.exit(1);
    });
}

function next_url(v: obj): string | undefined {
  let links = v.pagination.links;
  for (let i = 0; i < links.length; i++) {
    if (links[i].rel == "next") {
      return links[i].uri;
    }
  }
  return undefined
}

async function get_runs_at_offset(gameid: string, offset: number, count: number, recursive: boolean = false): Promise<any> {
  if (count < 0) {
    count = 200;
  }
  let url: string | undefined = runs_url(gameid, offset, count);
  if (recursive) {
    let zdata = [];
    while (url) {
      // @ts-ignore
      let n = parseInt(url.split("&").find(x => x.startsWith("offset")).split("=")[1]);
      console.log(`Getting ${gameid} runs at offset ${n} ...`);
      let v = await get_json(url);
      zdata.push(...v.data);
      url = next_url(v);
      if (url) {
        await sleep(1000);
      }
    }
    return zdata;
  }
  const res = await get_json(url);
  return res.data;
}

async function sync_runs(gameid: string, runs: obj[], n: number): Promise<obj[]> {
  if (n == 0) {
    console.log(`Getting ${gameid} runs at ${n} ...`);
    return get_runs_at_offset(gameid, 0, -1, true);
  }
  console.log(`Getting ${gameid} run at ${n - 1} ...`);
  let v = await get_runs_at_offset(gameid, n - 1, 1);
  if (v == undefined || v.length == 0 || v[0].id != runs[n - 1].id) {
    n = Math.max(0, Math.floor(n * 0.80));
    if (n < 100) {
      n = 0;
    }
    await sleep(1000);
    return sync_runs(gameid, runs, n);
  }
  console.log(`Getting ${gameid} runs at ${n} ...`);
  runs.splice(n);
  let extra = await get_runs_at_offset(gameid, n, -1, true);
  runs.push(...extra);
  return runs;
}

async function update_runs(gameid: string, out: string) {
  let runs0 = read_json(out);
  let runs1 = await sync_runs(gameid, runs0, runs0.length);
  fs.writeFileSync(out, JSON.stringify(runs1));
}

async function update_all_runs() {
  const games = read_json("botw.json").data;
  const output_files = ['botw_nor.json', 'botw_ext.json']
  let verifiers: obj = {};
  let players: obj = {};
  for (let i = 0; i < games.length; i++) {
    console.log(`Working on ${output_files[i]} ...`)
    await update_runs(games[i].id, output_files[i]);
    await sleep(1000);
    let runs = read_json(output_files[i]);
    runs.forEach((run: any) => {
      if ('examiner' in run.status) {
        verifiers[run.status.examiner] = 1;
      }
      run.players.data.forEach((player: any) => {
        if ('id' in player) {
          players[player.id] = 1;
        }
      });
    });
  }

  let missing = Object.keys(verifiers)
    .filter((ver: string) => (!(ver in players)));
  let out: obj = {};
  for (let i = 0; i < missing.length; i++) {
    let pid = missing[i];
    let url = `${api}/users/${pid}`;
    console.log(`Getting verifier ${pid} ${url}`);
    const res: obj = await get_json(url);
    if ('data' in res) {
      out[pid] = res.data;
    }
    await sleep(1000);
  }
  write_json("botw_users.json", out);

}


async function get_static_files() {
  const basic = [
    ["platforms.json", "platforms?max=200"],
    ["regions.json", "regions?max=200"],
    ["botw_ec_levels.json", "games/369pp381/levels"],
    ["botw_levels.json", "games/76rqjqd8/levels"],
  ];
  for (let i = 0; i < basic.length; i++) {
    let file = basic[i][0];
    let url = `${api}/${basic[i][1]}`;
    let res = await get_json(url);
    console.log(`Getting ${file} from ${url} ...`);
    write_json(file, res);
    await sleep(1000);
  }
  const catids = read_json("catids.db.json");
  let out: obj = {};
  for (let i = 0; i < catids.length; i++) {
    const cat = catids[i].id;
    let url = `${api}/categories/${cat}/variables`;
    console.log(`Getting ${cat} vars ${url} ...`);
    let res = await get_json(url);
    if ('data' in res) {
      out.cat = res.data;
    }
    await sleep(1000);
  }
  write_json("botw_cat_vars.json", out);
}

update_all_runs();
//get_static_files();
