const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');
const { chromium } = require('playwright');
const logins = require('./logins.json');


(async () => {
  const browser = await chromium.launch({ headless: true });

  for (const { username, password } of logins) {
    console.log(`🔐 Logging in as ${username}...`);

    const context = await browser.newContext();
    const page = await context.newPage();

    try {
      await page.goto('https://url.publishedprices.co.il/login');
      await page.fill('input[name="username"]', username);
      await page.fill('input[name="password"]', password || '');
      await Promise.all([
        page.waitForNavigation(),
        page.click('button[type="submit"]'),
      ]);

      const cookie = (await context.cookies()).find(c => c.name === 'cftpSID');
      const csrf = await page.getAttribute('meta[name="csrftoken"]', 'content');

      console.log(`📁 Fetching files for ${username}...`);
      const res = await page.request.post('https://url.publishedprices.co.il/file/json/dir', {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        form: {
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
          csrftoken: csrf,
        },
      });

      const json = await res.json();
      const files = json.aaData || [];

      const userDir = path.join(__dirname, 'downloads', username);
      fs.mkdirSync(userDir, { recursive: true });

      for (const file of files) {
        const fileUrl = `https://url.publishedprices.co.il/file/d/${file.fname}`;
        const filePath = path.join(userDir, file.fname);

        const downloadRes = await fetch(fileUrl, {
          headers: {
            Cookie: `cftpSID=${cookie.value}`,
          },
        });

        const buffer = await downloadRes.buffer();
        fs.writeFileSync(filePath, buffer);
        console.log(`✅ ${username}: ${file.fname}`);
      }

      await context.close();
    } catch (err) {
      console.error(`❌ Error with ${username}:`, err.message);
      await context.close();
    }
  }

  await browser.close();
  console.log('🎉 All done!');
})();
