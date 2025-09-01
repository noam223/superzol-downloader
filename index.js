// ייבוא מודולים נחוצים
import algoliasearch from 'algoliasearch';
import { XMLParser } from 'fast-xml-parser';
import zlib from 'zlib';
import fs from 'fs';
import path from 'path';
import fetch from 'node-fetch';
import dotenv from 'dotenv';
import { chromium } from 'playwright';

dotenv.config();

// התחברות ל-Algolia
const algoliaClient = algoliasearch(process.env.ALGOLIA_APP_ID, process.env.ALGOLIA_ADMIN_KEY);

// נתיב לקובץ עם פרטי הכניסה
const logins = JSON.parse(fs.readFileSync('./logins.json', 'utf-8'));
const parser = new XMLParser({ ignoreAttributes: false });
const BASE_URL = 'https://url.publishedprices.co.il';

/**
 * פונקציה שמאתרת את הקובץ העדכני ביותר מכל סוג (מחיר/מבצע/חנויות) עבור כל חנות/רשת.
 *
 * @param {array} fileList - רשימת הקבצים שהתקבלה מהאתר.
 * @returns {array} - מערך של אובייקטי הקבצים העדכניים ביותר.
 */
const getLatestFiles = (fileList) => {
    const map = new Map();
    for (const file of fileList) {
        const match = file.fname.match(/^(PriceFull|Price|PromoFull|Promo|Stores)(\d+)(?:-(\d+))?-\d{12}\.gz$/i);
        if (!match) continue;
        const [_, type, chainId, storeId] = match;
        const key = `${type.toLowerCase()}_${storeId || chainId}`; // Store files use chainId as key
        const existing = map.get(key);
        if (!existing || file.ftime > existing?.ftime) {
            map.set(key, { ...file, type: type.toLowerCase(), chainId, storeId });
        }
    }
    return Array.from(map.values());
};

/**
 * מנתחת קובץ XML של נתוני חנויות ומחזירה מערך של רשומות חנויות.
 *
 * @param {string} xmlContent - תוכן קובץ ה-XML כסטרינג.
 * @returns {array} - מערך של רשומות חנויות.
 */
function parseXmlStoreFile(xmlContent) {
    const parser = new XMLParser({ ignoreAttributes: false });
    const data = parser.parse(xmlContent);

    const chainId = String(data?.Root?.ChainID || data?.Root?.ChainId);
    const chainName = data?.Root?.ChainName;
    const stores = data?.Root?.SubChains?.SubChain?.Stores?.Store;

    if (!chainId || !stores) return [];

    const storeList = Array.isArray(stores) ? stores : [stores];
    return storeList.map(store => {
        const rawId = parseInt(store.StoreID || store.StoreId);
        const storeId = rawId.toString().padStart(3, '0');
        return {
            chain_id: chainId,
            store_id: storeId,
            object_id_suffix: `${storeId}`,
            store_name: `${chainName} - ${store.StoreName}`,
        };
    });
}

/**
 * בודק אם לפריט יש מבצע פעיל.
 *
 * @param {object} hit - אובייקט של מוצר מ-Algolia.
 * @returns {boolean} - האם יש מבצע פעיל.
 */
function isPromotion(hit) {
    const promotionId = hit.PromotionId;
    const discounted = parseFloat(hit.DiscountedPrice || 0);
    const itemPrice = parseFloat(hit.ItemPrice || 0);
    return (
        (promotionId && `${promotionId}`.trim() !== '') ||
        (discounted > 0 && discounted < itemPrice)
    );
}

/**
 * סורק את האינדקסים של החנויות ומעדכן את סטטוס המבצעים באינדקס הראשי.
 */
async function updateGlobalPromotionStatus() {
    const statusMap = new Map();
    const indexNames = [
        'products_7291059100008', 'products_7290803800003', 'products_7290103152017', 
        'products_7290873255550', 'products_7290639000004', 'products_7290058140886', 
        'products_7290526500006', 'products_7290055700008', 'products_7290839800000', 
        'products_7290555555555', 
    ];

    console.log('\n🔄 מתחיל סנכרון סטטוס מבצעים לאינדקס הגלובלי...');

    for (const indexName of indexNames) {
        const index = algoliaClient.initIndex(indexName);
        console.log(`🔍 סורק אינדקס: ${indexName}`);

        await index.browseObjects({
            query: '',
            batch: batch => {
                batch.forEach(hit => {
                    const pid = hit.ItemCode;
                    if (!pid) return;

                    const hasPromo = isPromotion(hit);
                    if (hasPromo) {
                        statusMap.set(pid, true);
                    } else if (!statusMap.has(pid)) {
                        statusMap.set(pid, false);
                    }
                });
            },
        });
    }

    const updates = [];
    for (const [productId, hasPromo] of statusMap.entries()) {
        updates.push({
            objectID: productId,
            has_promotion: hasPromo,
        });
    }

    const productsIndexGlobal = algoliaClient.initIndex('products_index');
    console.log(`🚀 מעדכן ${updates.length} מוצרים באינדקס הראשי...`);
    await productsIndexGlobal.partialUpdateObjects(updates, { createIfNotExists: false });
    console.log(`✅ עודכנו ${updates.length} מוצרים`);
}

(async () => {
    const productsIndexGlobal = algoliaClient.initIndex('products_index');
    const globalMap = new Map();
    const storeInfoMap = new Map();
    
    const browser = await chromium.launch({ headless: true });

    try {
        // לולאה שעוברת על כל המשתמשים שהוגדרו בקובץ logins.json
        for (const { username, password } of logins) {
            console.log(`\n🔐 מתחבר כמשתמש ${username}...`);
            
            // הוספת ignoreHTTPSErrors: true כדי להתעלם משגיאות SSL/TLS
            const context = await browser.newContext({ ignoreHTTPSErrors: true });
            const page = await context.newPage();

            try {
                // ניווט והתחברות באמצעות Playwright
                await page.goto('https://url.publishedprices.co.il/login');
                await page.fill('input[name="username"]', username);
                await page.fill('input[name="password"]', password || '');

                await Promise.all([
                    page.waitForNavigation(),
                    page.click('button[type="submit"]'),
                ]);
                
                // חילוץ קוקיז וטוקן CSRF מהדף
                const cookie = (await context.cookies()).find(c => c.name === 'cftpSID');
                const csrf = await page.getAttribute('meta[name="csrftoken"]', 'content');
                
                console.log(`📁 מביא רשימת קבצים עבור ${username}...`);

                const fileListRes = await page.request.post(`${BASE_URL}/file/json/dir`, {
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    },
                    form: {
                        sEcho: '1', iColumns: '5', sColumns: ',,,,',
                        iDisplayStart: '0', iDisplayLength: '100000',
                        mDataProp_0: 'fname', mDataProp_1: 'typeLabel',
                        mDataProp_2: 'size', mDataProp_3: 'ftime',
                        mDataProp_4: '', sSearch: '', bRegex: 'false',
                        iSortingCols: '0', cd: '/', csrftoken: csrf,
                    },
                });

                const fileList = (await fileListRes.json()).aaData || [];
                const latestFiles = getLatestFiles(fileList);

                console.log(`⬇️ ${username}: מוריד ${latestFiles.length} קבצים...`);

                const userDir = path.join(__dirname, 'downloads', username);
                fs.rmSync(userDir, { recursive: true, force: true });
                fs.mkdirSync(userDir, { recursive: true });

                for (const file of latestFiles) {
                    const fileUrl = `https://url.publishedprices.co.il/file/d/${file.fname}`;
                    const filePath = path.join(userDir, file.fname);
                    
                    const downloadRes = await fetch(fileUrl, {
                        headers: {
                            Cookie: `cftpSID=${cookie.value}`,
                        },
                    });
                    
                    const buffer = await downloadRes.buffer();
                    fs.writeFileSync(filePath, buffer);
                    console.log(`✅ ${username}: הוריד ${file.fname}`);
                }
                await context.close();
            } catch (err) {
                console.error(`❌ שגיאה עם המשתמש ${username}:`, err.message);
                await context.close();
                continue; // עובר למשתמש הבא במקרה של שגיאה
            }
            
            // עכשיו נעבד את הקבצים שהורדו
            const downloadedFiles = fs.readdirSync(userDir);
            for (const fname of downloadedFiles) {
                const fullPath = path.join(userDir, fname);
                const buffer = fs.readFileSync(fullPath);
                const xml = zlib.gunzipSync(buffer).toString('utf8');
                
                const match = fname.match(/^(PriceFull|Price|PromoFull|Promo|Stores)(\d+)(?:-(\d+))?-\d{12}\.gz$/i);
                if (!match) continue;

                const [_, type, chainId, storeId] = match;

                if (type.toLowerCase().startsWith('price')) {
                    const json = parser.parse(xml);
                    const index = algoliaClient.initIndex(`products_${chainId}_${storeId}`);
                    let items = json?.Root?.Items?.Item || [];
                    if (!Array.isArray(items)) items = [items];
                    
                    const itemsToUpload = items.map(p => {
                        const formatted = {
                            objectID: `${p.ItemCode}-${storeId}`,
                            ...p,
                        };
                        if (!globalMap.has(p.ItemCode)) {
                            const { StoreId, ChainId, ItemPrice, UnitOfMeasurePrice, PriceUpdateDate,
                                PromotionId, PromotionDescription, PromotionUpdateDate,
                                PromotionStartDate, PromotionStartHour, PromotionEndDate,
                                PromotionEndHour, MinQty, DiscountedPrice, DiscountedPricePerMida,
                                MinNoOfItemOfered, StoreName, ...cleaned
                            } = formatted;
                            globalMap.set(p.ItemCode, {
                                objectID: p.ItemCode,
                                ...cleaned,
                            });
                        }
                        return formatted;
                    });

                    console.log(`🚀 מעלה ${itemsToUpload.length} מוצרים לאינדקס ${index.indexName}`);
                    await index.saveObjects(itemsToUpload);

                } else if (type.toLowerCase().startsWith('promo')) {
                    const json = parser.parse(xml);
                    const index = algoliaClient.initIndex(`products_${chainId}_${storeId}`);
                    let promotions = json?.Root?.Promotions?.Promotion || [];
                    if (!Array.isArray(promotions)) promotions = [promotions];
                    
                    const itemsToUpload = [];
                    for (const promo of promotions) {
                        let products = promo?.PromotionItems?.Item || [];
                        if (!Array.isArray(products)) products = [products];
                        for (const product of products) {
                            itemsToUpload.push({
                                objectID: `${product.ItemCode}-${storeId}`,
                                PromotionId: promo.PromotionId,
                                PromotionDescription: promo.PromotionDescription,
                                PromotionUpdateDate: promo.PromotionUpdateDate,
                                PromotionStartDate: promo.PromotionStartDate,
                                PromotionEndHour: promo.PromotionEndHour,
                                MinQty: promo.MinQty,
                                DiscountedPrice: promo.DiscountedPrice,
                                DiscountedPricePerMida: promo.DiscountedPricePerMida,
                                MinNoOfItemOfered: promo.MinNoOfItemOfered,
                            });
                            }
                        }

                        if (itemsToUpload.length > 0) {
                            console.log(`🔥 מעדכן ${itemsToUpload.length} פריטי מבצעים באינדקס ${index.indexName}`);
                            await index.partialUpdateObjects(itemsToUpload, { createIfNotExists: true });
                        }
                } else if (type.toLowerCase().startsWith('stores')) {
                    const storeRecords = parseXmlStoreFile(xml);
                    storeInfoMap.set(chainId, storeRecords);
                    console.log(`📦 נטענו ${storeRecords.length} סניפים עבור רשת ${chainId}`);
                }
            }

        }
    } finally {
        await browser.close();
    }

    // לאחר עיבוד כל הקבצים, מעלים את הנתונים לאינדקס הגלובלי ומעדכנים את שמות החנויות.
    const globalFormatted = Array.from(globalMap.values());
    if (globalFormatted.length) {
        await productsIndexGlobal.saveObjects(globalFormatted);
        console.log(`🌍 הועלו ${globalFormatted.length} מוצרים לאינדקס הגלובלי (products_index)`);
    }

    console.log(`\n🔄 מתחיל סנכרון שמות חנויות...`);
    for (const [chainId, records] of storeInfoMap.entries()) {
        const indexName = `products_${chainId}`;
        const index = algoliaClient.initIndex(indexName);
        const updated = [];
        
        await index.browseObjects({
            batch: batch => {
                for (const item of batch) {
                    const suffix = item.store_id?.toString().padStart(3, '0');
                    const match = records.find(r => r.object_id_suffix === suffix);
                    if (match) {
                        updated.push({
                            objectID: item.objectID,
                            store_name: match.store_name,
                        });
                    }
                }
            },
        });

        if (updated.length > 0) {
            await index.partialUpdateObjects(updated, { createIfNotExists: false });
            console.log(`✅ עודכנו ${updated.length} פריטים ב־${indexName}`);
        } else {
            console.log(`ℹ️ אין מה לעדכן ב־${indexName}`);
        }
    }
    console.log('🏁 סיום עדכון שמות חנויות ב-Algolia.');
    await updateGlobalPromotionStatus();

    console.log('✅ כל הרשתות סונכרנו בהצלחה.');
})();
