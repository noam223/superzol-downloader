process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0'; // עקיפת SSL שפג

const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');
const { CookieJar } = require('tough-cookie');
const fetchCookie = require('fetch-cookie');
const https = require('https');
const logins = require('./logins.json');

const agent = new https.Agent({ rejectUnauthorized: false });

const loginAndDownload = async (username, password) => {
  const jar = new CookieJar();
  const fetchWithCookies = fetchCookie(fetch, jar);

  console.log(`🔐 Logging in as ${username}...`);

  // התחברות
  const loginRes = await fetchWithCookies('https://url.publishedprices.co.il/login', {
    method: 'POST',
    agent,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: new URLSearchParams({
      username,
      password: password || '',
    }),
  });

  const loginText = await loginRes.text();
  if (!loginText.includes('window.location')) {
    console.error(`❌ Login failed for ${username}`);
    return;
  }

  // בקשת רשימת קבצים
  const csrfRes = await fetchWithCookies('https://url.publishedprices.co.il/', { agent });
  const csrfHtml = await csrfRes.text();
  const csrfMatch = csrfHtml.match(/name="csrftoken" content="(.+?)"/);
  const csrfToken = csrfMatch ? csrfMatch[1] : null;

  if (!csrfToken) {
    console.error(`❌ CSRF token not found for ${username}`);
    return;
  }

  console.log(`📁 Fetching files for ${username}...`);

  const res = await fetchWithCookies('https://url.publishedprices.co.il/file/json/dir', {
    method: 'POST',
    agent,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    },
    body: new URLSearchParams({
      sEcho: '1',
      iColumns: '5',
      sColumns: ',,,,',
      iDisplayStart: '0',
      iDisplayLength: '100000',
      mDataProp_0: 'fname',
      sSearch_0: '',
      bRegex_0: 'false',
      bSearchable_0: 'true',
      bSortable_0: 'true',
      mDataProp_1: 'typeLabel',
      sSearch_1: '',
      bRegex_1: 'false',
      bSearchable_1: 'true',
      bSortable_1: 'false',
      mDataProp_2: 'size',
      sSearch_2: '',
      bRegex_2: 'false',
      bSearchable_2: 'true',
      bSortable_2: 'true',
      mDataProp_3: 'ftime',
      sSearch_3: '',
      bRegex_3: 'false',
      bSearchable_3: 'true',
      bSortable_3: 'true',
      mDataProp_4: '',
      sSearch_4: '',
      bRegex_4: 'false',
      bSearchable_4: 'true',
      bSortable_4: 'false',
      sSearch: '',
      bRegex: 'false',
      iSortingCols: '0',
      cd: '/',
      csrftoken: csrfToken,
    }),
  });

  const json = await res.json();
  const files = json.aaData || [];

  const userDir = path.join(__dirname, 'downloads', username);
  fs.mkdirSync(userDir, { recursive: true });

  for (const file of files) {
    const fileUrl = `https://url.publishedprices.co.il/file/d/${file.fname}`;
    const filePath = path.join(userDir, file.fname);

    const downloadRes = await fetchWithCookies(fileUrl, { agent });
    const buffer = await downloadRes.buffer();
    fs.writeFileSync(filePath, buffer);
    console.log(`✅ ${username}: ${file.fname}`);
  }
};

(async () => {
  for (const { username, password } of logins) {
    try {
      await loginAndDownload(username, password);
    } catch (err) {
      console.error(`❌ Error with ${username}:`, err.message);
    }
  }

  console.log('🎉 All chains processed.');
})();
