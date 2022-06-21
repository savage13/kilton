// Build the database from input json and sql files
// Input:
//   - src2.sql            - User defined
//   - src2_post.sql       - User defined
//   - botw_nor.json       - Updated by get_runs daily
//   - botw_ext.json       - Updated by get_runs daily
//   - botw_users.json     - Updated by get_runs daily
//   - botw.json           - Static
//   - catids.db.json      - Static
//   - botw_cat_vars.json  - Update by get_runs (randomly)
//   - botw_levels.json    - Update by get_runs (randomly)
//   - botw_ec_levels.json - Update by get_runs (randomly)
//   - regions.json        - Update by get_runs (randomly)
//   - platforms.json      - Update by get_runs (randomly)
//
// Output:
//  - botw.db

import sqlite3 from 'better-sqlite3';

type obj = { [key: string]: any };

var fs = require('fs');

const db = sqlite3('botw.db');

function run_sql(file: string, db: any) {
  const sql = fs.readFileSync(file, 'utf8')
  db.exec(sql);
}
function read_json(file: string) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

// Generate an insert statement for a table
function gen_insert(table: string, db: any): any {
  let stmt = db.prepare(`SELECT * from ${table} limit 0`);
  let cols = stmt.columns();
  let keys = cols.map((col: any) => col.name).join(", ");
  let vals = cols.map((col: any) => "@" + col.name).join(", ");
  return db.prepare(`INSERT into ${table} (${keys}) values (${vals})`);
}
// Generate an insert a statement for a table in a transaction
//   Transctions are much faster
function gen_insert_t(table: string, db: any): any {
  let stmt = gen_insert(table, db);
  return db.transaction((rows: any) => { for (const row of rows) stmt.run(row) });
}

function bool_to_int(value: boolean): number {
  return (value) ? 1 : 0;
}

function src_run_to_run(run: { [key: string]: any }): { [key: string]: any } {
  let r: any = {};
  r.id = run.id;
  r.game = run.game;
  r.category = run.category;
  r.level = run.level;
  r.date = run.date;
  r.submitted = run.submitted;
  r.comment = run.comment;
  r.platform = run.system.platform;
  r.region = run.system.region;
  r.emulated = (run.system.emulated) ? 1 : 0;
  r.examiner = run.status.examiner;
  r.verify_date = run.status.verify_date || null;
  r.reason = run.status.reason || null;
  r.status = run.status.status;
  r.realtime_t = run.times.realtime_t;
  r.realtime_t = run.times.realtime_t;
  r.primary_t = run.times.primary_t;
  r.primary_ = run.times.primary;
  r.realtime = run.times.realtime;
  r.realtime_noloads_t = run.times.realtime_noloads_t;
  r.realtime_noloads = run.times.realtime_noloads;
  r.ingame = run.times.ingame;
  r.ingame_t = run.times.ingame_t;
  r.video_text = '';
  return r;
}


function src_run_to_run_players(run: obj): obj[] {
  let rid = run.id;
  return run.players.data
    .filter((player: any) => (player && player.id))
    .map((player: any) => { return { rid, pid: player.id }; });
}
function src_run_to_run_values(run: obj): obj[] {
  let rid = run.id;
  let out: obj[] = [];
  for (const [kid, vid] of Object.entries(run.values)) {
    out.push({ rid, kid, vid });
  }
  return out;
}
function src_run_to_run_videos(run: obj): obj[] {
  if (!run.videos || !run.videos.links) {
    return [];
  }
  return run.videos.links
    .filter((link: any) => (link && link.uri))
    .map((link: any) => { return { rid: run.id, uri: link.uri } });
}

function location(loc: obj, kind: string): [string | null, string | null, string | null] {
  if (!loc || !loc[kind]) {
    return [null, null, null];
  }
  let v = loc[kind];
  let code = v.code || null;
  if (!v.names) {
    return [code, null, null];
  }
  return [code, v.names.international, v.names.japanese];
}

function get_style(s: obj): [string | null,
  string | null, string | null, string | null,
  string | null, string | null, string | null] {
  if (!s || !s['name-style']) {
    return [null, null, null, null, null, null, null];
  }
  let ns = s['name-style']
  let kind = ns.style;
  if (kind == "gradient") {
    return [kind, ns['color-from'].light, ns['color-from'].dark,
      ns['color-to'].light, ns['color-to'].dark, null, null]
  }
  return [kind, null, null, null, null, ns.color.light, ns.color.dark]
}
function src_run_to_players_links(run: obj): obj[] {
  return run.players.data.map((p: any) => src_player_to_player_links(p));
}
function src_player_to_player_links(p: obj): obj[] | null {
  if (!p || !p.id) {
    return null;
  }
  let names = ['youtube', 'twitch', 'hitbox', 'speedrunslive', 'twitter'];
  return names.map(name => [name, p[name]])
    .filter(v => v[1])
    .map(v => {
      return {
        pid: p.id,
        uri: v[1].uri,
        name: v[0]
      }
    });
}

// Assumes players were retrieved with 'embed'
function src_run_to_players(run: obj): obj[] {
  return run.players.data.map((p: any) => src_player_to_player(p));
}
function src_player_to_player(p: obj): obj | null {
  if (!p || !p.id) {
    return null;
  }
  let [style, color_from_light, color_from_dark,
    color_to_light, color_to_dark, color_light, color_dark] =
    get_style(p);
  let namei = null;
  let namej = null;
  if (p.names) {
    namei = p.names.international;
    namej = p.names.japanese;
  }
  let [loc_code, loc_namei, loc_namej] = location(p.location, "country");
  let [reg_code, reg_namei, reg_namej] = location(p.location, "region");
  return {
    id: p.id, namei, namej, pronouns: p.pronouns, role: p.role,
    weblink: p.weblink, signup: p.signup,
    loc_code, loc_namei, loc_namej,
    reg_code, reg_namei, reg_namej,
    style, color_light, color_dark,
    color_from_light, color_from_dark,
    color_to_light, color_to_dark,
  };
}


function src_category_to_category(cat: obj, game_id: string): obj {
  return {
    game: game_id,
    category: cat.name,
    id: cat.id,
    type: cat.type,
    miscellaneous: (cat.miscellaneous) ? 1 : 0,
    rules: cat.rules,
    weblink: cat.weblink,
    players: JSON.stringify(cat.players),
  };
}

function src_variable_to_variable(v: obj): obj {
  return {
    id: v.id,
    variable: v.name,
    category: v.category,
    default_id: v.values.default,
    type: v.scope.type,
    level: v.scope.level,
    mandatory: bool_to_int(v.mandatory),
    user_defined: bool_to_int(v['user-defined']),
    obsoletes: bool_to_int(v.obsoletes),
    is_subcategory: bool_to_int(v['is-subcategory']),
  };
}
function src_variable_to_vals(v: obj): obj[] {
  let variable_id = v.id;
  return Object.keys(v.values.values).map((id: any) => {
    let value = v.values.values[id];
    return {
      vid: variable_id,
      id: id,
      label: value.label,
      rules: value.rules,
      miscellaneous: (value.flags) ? bool_to_int(value.flags.miscellaneous) : null,
    };
  });
}

const insert_run = gen_insert_t("runs", db);
const insert_run_videos = gen_insert_t("run_videos", db);
const insert_run_values = gen_insert_t("run_values", db);
const insert_run_players = gen_insert_t("run_players", db);
const insert_players = gen_insert_t("players", db);
const insert_player_links = gen_insert_t("player_links", db);
const insert_category = gen_insert_t("categories", db);
const insert_variable = gen_insert_t("variables", db);
const insert_vals = gen_insert_t("vals", db);
const cats_insert = gen_insert_t("cat_vars", db);
const level_insert = gen_insert_t("levels", db);
const regions_insert = gen_insert_t("regions", db);
const plats_insert = gen_insert_t("platforms", db);

console.log("Setup database ...");
run_sql('./src2.sql', db);

let players: obj = {};
let links: obj = {};

function load_runs(file: string) {
  console.log(`Working on ${file} ...`);
  const runs = read_json(file);

  console.log(`  runs ${runs.length} ...`);
  insert_run(runs.map(src_run_to_run));

  let rp = runs.flatMap(src_run_to_run_players).filter((row: any) => row);
  insert_run_players(rp);

  let values = runs.flatMap(src_run_to_run_values);
  let videos = runs.flatMap(src_run_to_run_videos);

  insert_run_values(values);
  insert_run_videos(videos);

  runs.forEach((run: any) => {
    src_run_to_players(run).filter(p => p).forEach((p: obj) => {
      players[p.id] = p;
    });
  });

  runs.forEach((run: any) => {
    src_run_to_players_links(run).filter(p => p).forEach((plink: obj) => {
      plink.forEach((v: any) => {
        links[v.pid + ':' + v.name] = v;
      });
    });
  });
}

load_runs("botw_nor.json");
load_runs("botw_ext.json");

// Verifiers (players without runs)
let verifiers = read_json('botw_users.json');
for (const [id, p] of Object.entries(verifiers)) {
  let v = src_player_to_player(p as obj);
  if (p && v) {
    players[v.id] = v;
    // @ts-ignore
    src_player_to_player_links(p as obj).forEach((v: any) => {
      links[v.pid + ':' + v.name] = v;
    });
  }
}
insert_players(Object.values(players));
insert_player_links(Object.values(links));

console.log("Static data ...");

console.log("  categories, variables, and values ...");
let games = read_json('botw.json').data;
games.forEach((game: any) => {
  insert_category(game.categories.map((v: any) => src_category_to_category(v, game.id)));
  insert_variable(game.variables.map(src_variable_to_variable));
  insert_vals(game.variables.flatMap(src_variable_to_vals));
});

console.log("  cat_vars ...");
let cats = read_json('botw_cat_vars.json');
for (const [cid, vals] of Object.entries(cats)) {
  // @ts-ignore , eek
  cats_insert(vals.map((val: any) => { return { cid, vid: val.id } }));
}

console.log("  levels ...");
['botw_levels.json', 'botw_ec_levels.json'].map((file: string) => {
  const levels = read_json(file).data;
  level_insert(levels.map((x: any) => { return { lid: x.id, level: x.name } }));
});

console.log("  regions ...");
const regions = read_json('regions.json').data;
regions_insert(regions.map((reg: any) => { return { id: reg.id, region: reg.name } }));

console.log("  platforms ...");
const plats = read_json('platforms.json').data;
function platforms(p: any): obj {
  return { id: p.id, platform: p.name, released: p.released };
}
plats_insert(plats.map(platforms));

console.log("post processing ... ");
run_sql('./src2_post.sql', db);

function check_table(table: string, n: number) {
  let stmt = db.prepare(`select * from ${table}`);
  let v = stmt.all([]);
  if (v.length != n) {
    console.error(`${table} size incorrect ${v.length} ${n}`);
  }
}

// check_table("cat_vars", 207);
// check_table("categories", 42);
// check_table("games", 2);
// check_table("levels", 37);
// check_table("platforms", 177);
// check_table("player_links", 2198);
// check_table("players", 1422);
// check_table("regions", 6);
// check_table("run_players", 9509);
// check_table("run_values", 26874);
// check_table("run_videos", 9989);
// check_table("runs", 9540);
// check_table("vals", 232);
// check_table("variables", 63);
