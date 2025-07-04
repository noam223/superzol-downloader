console.log('🟢 האפליקציה עלתה – ללא הורדה אוטומטית.');

if (process.env.FORCE_DOWNLOAD === 'true') {
  console.log('🔁 מופעלת הורדת קבצים לפי משתנה סביבה...');
  require('./download.js');
}
