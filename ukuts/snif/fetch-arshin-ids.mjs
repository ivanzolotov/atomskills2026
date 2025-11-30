// Node 18+ / 20+ / 22+
//
// Установка зависимостей:
//   npm i axios tough-cookie axios-cookiejar-support
//
// Запуск:
//   node fetch-arshin-ids-csv.mjs --csv=queries.csv --rows=50 --minDelay=3000 --maxDelay=9000 --timeout=60000
//
// Вход: CSV с заголовком query,year
// Выход: ids.json, ids.csv и progress.json (resume)

import fs from 'node:fs/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import axios from 'axios';
import { CookieJar } from 'tough-cookie';
import { wrapper } from 'axios-cookiejar-support';

function arg(name, def) {
  const m = process.argv.find(a => a.startsWith(`--${name}=`));
  if (!m) return def;
  return m.slice(name.length + 3);
}

const CSV_FILE    = arg('csv', 'queries.csv');
const ROWS        = parseInt(arg('rows', '50'), 10);
const MIN_DELAY   = parseInt(arg('minDelay', '3000'), 10);
const MAX_DELAY   = parseInt(arg('maxDelay', '9000'), 10);
const MAX_RETRIES = parseInt(arg('retries', '5'), 10);
const TIMEOUT_MS  = parseInt(arg('timeout', '60000'), 10);
const MAX_PAGES   = parseInt(arg('maxPages', '0'), 10); // 0 = без лимита

const BASE = 'https://fgis.gost.ru';
const RESULTS_PAGE = `${BASE}/fundmetrology/cm/results`;
const ENDPOINT = `${BASE}/fundmetrology/cm/xcdb/vri/select`;

const PROGRESS_FILE = 'progress.json';
const OUT_JSON = 'ids.json';
const OUT_CSV  = 'ids.csv';

const rand = (a, b) => Math.floor(Math.random() * (b - a + 1)) + a;
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));

/** CSV-парсер (простой, с поддержкой кавычек) */
function parseCSV(text) {
  const rows = [];
  let i = 0, field = '', row = [], inQuotes = false;
  const pushField = () => { row.push(field); field = ''; };
  const pushRow = () => { rows.push(row.map(s => s.trim())); row = []; };

  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1); // BOM

  while (i < text.length) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
        inQuotes = false; i++; continue;
      }
      field += ch; i++; continue;
    }
    if (ch === '"') { inQuotes = true; i++; continue; }
    if (ch === ',') { pushField(); i++; continue; }
    if (ch === '\n') { pushField(); pushRow(); i++; continue; }
    if (ch === '\r') { if (text[i + 1] === '\n') i++; pushField(); pushRow(); i++; continue; }
    field += ch; i++;
  }
  if (field.length || row.length) { pushField(); pushRow(); }
  return rows;
}

async function loadCSVRecords(file) {
  const raw = await fs.readFile(file, 'utf8');
  const rows = parseCSV(raw).filter(r => r.length && r.some(x => x !== ''));
  if (!rows.length) return [];
  const header = rows[0].map(h => h.toLowerCase());
  const qi = header.indexOf('query');
  const yi = header.indexOf('year');
  if (qi === -1) throw new Error('В CSV нет столбца "query"');

  const recs = [];
  for (let k = 1; k < rows.length; k++) {
    const r = rows[k];
    const query = (r[qi] || '').trim();
    const year  = yi >= 0 ? (r[yi] || '').trim() : '';
    if (!query) continue;
    recs.push({ query, year });
  }
  return recs;
}

async function loadProgress() {
  try { return JSON.parse(await fs.readFile(PROGRESS_FILE, 'utf8')); }
  catch { return { index: 0, results: [] }; }
}
async function saveProgress(state) {
  await fs.writeFile(PROGRESS_FILE, JSON.stringify(state, null, 2), 'utf8');
}

function buildParams(query, year, start = 0, rows = 20) {
  const p = new URLSearchParams();
  p.append('fq', `*${query}*`);
  if (year) p.append('fq', `verification_year:${year}`);
  p.set('q', '*');
  p.set('fl', 'vri_id,org_title,mi.mitnumber,mi.mititle,mi.mitype,mi.modification,mi.number,verification_date,valid_date,applicability,result_docnum,sticker_num');
  p.set('sort', 'verification_date desc,org_title asc');
  p.set('rows', String(rows));
  p.set('start', String(start));
  return p;
}

async function createClientWithCookies() {
  const jar = new CookieJar();
  const client = wrapper(axios.create({
    jar,
    withCredentials: true,
    timeout: TIMEOUT_MS,
    headers: {
      'Accept': 'application/json, text/plain, */*',
      'Accept-Language': 'ru,en;q=0.9',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140 Safari/537.36',
      // реферер добавим на уровне запроса
    }
  }));

  // «Прогрев»: получить session-cookie как браузер
  try {
    await client.get(RESULTS_PAGE, {
      headers: {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
      }
    });
  } catch (e) {
    // даже если страница отдала 200/304/и т.п. — ок; если упало — попробуем дальше, ретраи покроют
  }

  return client;
}

async function getPage(client, params, attempt = 0) {
  try {
    const res = await client.get(ENDPOINT, {
      params,
      headers: {
        'Referer': RESULTS_PAGE,
        // иногда помогает:
        'X-Requested-With': 'XMLHttpRequest',
      }
    });
    return res.data;
  } catch (err) {
    const status = err.response?.status;
    if (attempt >= MAX_RETRIES) throw err;
    if (status === 405 || status === 403 || status === 429) {
      // жёсткий backoff
      const backoff = clamp(2000 * Math.pow(2, attempt) + rand(0, 1000), 2000, 60000);
      console.warn(`  -> ошибка запроса (HTTP ${status || '??'}), backoff ${backoff} мс, попытка ${attempt + 1}/${MAX_RETRIES}`);
      await sleep(backoff);
      return getPage(client, params, attempt + 1);
    }
    // сетевые и прочие
    const backoff = clamp(1500 * Math.pow(2, attempt) + rand(0, 800), 1000, 45000);
    console.warn(`  -> ошибка запроса (${err.message}), backoff ${backoff} мс, попытка ${attempt + 1}/${MAX_RETRIES}`);
    await sleep(backoff);
    return getPage(client, params, attempt + 1);
  }
}

async function collectIdsFor(client, query, year) {
  let start = 0;
  const rows = ROWS;
  const ids = new Set();
  let pages = 0;
  let numFound = 0;
  let printedEst = false;

  while (true) {
    if (MAX_PAGES > 0 && pages >= MAX_PAGES) {
      console.warn(`  -> достигнут лимит maxPages=${MAX_PAGES}, останавливаем пагинацию`);
      break;
    }

    console.log(`  страница ${pages + 1}, start=${start} …`);
    const params = buildParams(query, year, start, rows);
    const data = await getPage(client, params);
    const resp = data?.response;
    if (!resp) { console.warn('  -> пустой ответ/нет response'); break; }

    numFound = typeof resp.numFound === 'number' ? resp.numFound : numFound;
    if (!printedEst) {
      const est = rows > 0 ? Math.ceil(numFound / rows) : 0;
      console.log(`  numFound=${numFound}, rows=${rows}, ~страниц=${est || '?'}`);
      printedEst = true;
    }

    const docs = Array.isArray(resp.docs) ? resp.docs : [];
    for (const d of docs) if (d && d.vri_id) ids.add(String(d.vri_id));

    pages++;
    start += rows;

    if (start < numFound) {
      const pause = rand(MIN_DELAY, MAX_DELAY);
      await sleep(pause);
    } else {
      break;
    }
  }

  return { ids: Array.from(ids), pages, numFound };
}

(async () => {
  const records = await loadCSVRecords(CSV_FILE);
  if (!records.length) {
    console.error('CSV пуст или без корректного заголовка query,year');
    process.exit(1);
  }

  const client = await createClientWithCookies();

  const state = await loadProgress();
  const doneMap = new Map((state.results || []).map(r => [`${r.query}::${r.year}`, r]));
  const out = [];
  let csvLines = ['query,year,vri_id'];
  const startIndex = state.index || 0;

  console.log(`Всего запросов: ${records.length}. Начинаем с ${startIndex + 1}.`);

  for (let i = startIndex; i < records.length; i++) {
    const { query, year } = records[i];
    const key = `${query}::${year || ''}`;

    if (doneMap.has(key)) {
      const rec = doneMap.get(key);
      out.push(rec);
      rec.ids.forEach(id => csvLines.push(`"${query.replace(/"/g,'""')}","${(year||'').replace(/"/g,'""')}","${id}"`));
      continue;
    }

    console.log(`\n[${i + 1}/${records.length}] "${query}"  year=${year || '(любой)'}`);
    const t0 = Date.now();
    const { ids, pages, numFound } = await collectIdsFor(client, query, year);
    const dt = ((Date.now() - t0) / 1000).toFixed(1);
    console.log(`  найдено: ${ids.length} id (numFound=${numFound}, страниц=${pages}), за ${dt} c`);

    const rec = { query, year: year || '', ids, found: ids.length, pages, numFound };
    out.push(rec);
    state.results = out;
    state.index = i + 1;
    await saveProgress(state);

    ids.forEach(id => csvLines.push(`"${query.replace(/"/g,'""')}","${(year||'').replace(/"/g,'""')}","${id}"`));

    const pause = rand(MIN_DELAY, MAX_DELAY);
    console.log(`  пауза ${pause} мс …`);
    await sleep(pause);
  }

  await fs.writeFile(OUT_JSON, JSON.stringify(out, null, 2), 'utf8');
  await fs.writeFile(OUT_CSV, csvLines.join('\n') + '\n', 'utf8');

  console.log(`\nГотово:
  • ${OUT_JSON}
  • ${OUT_CSV}
  • ${PROGRESS_FILE}`);
})().catch(err => {
  console.error('Фатальная ошибка:', err);
  process.exit(2);
});