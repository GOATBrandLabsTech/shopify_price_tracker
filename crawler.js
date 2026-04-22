require('dotenv').config({
    path: '/home/ubuntu/project/shopify_price_tracker/.env',
    override: true
});
const puppeteer = require('puppeteer');
const { Pool } = require('pg');

// Database connection using env variables
const pool = new Pool({
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});

const getSlugsFromDb = async () => {
    const client = await pool.connect();
    try {
        const query = `SELECT slug FROM "DataWarehouse".price_monitoring_shopify_slugs WHERE brand = 'Chumbak'`;
        const res = await client.query(query);
        return res.rows.map(row => row.slug);
    } finally {
        client.release();
    }
};

const insertScrapedData = async (data) => {
    const client = await pool.connect();
    try {
        // Upsert logic (Insert or Update if slug already exists)
        // Note: Assumes `slug` is a Primary Key or UNIQUE constraint in dbt_dsarkar.shopify_pricing.
        // If it isn't, standard INSERT is used without ON CONFLICT.
        const query = `
            INSERT INTO dbt_dsarkar.shopify_pricing (slug, product_name, selling_price, mrp, in_stock, product_id, scraped_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (slug) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                selling_price = EXCLUDED.selling_price,
                mrp = EXCLUDED.mrp,
                in_stock = EXCLUDED.in_stock,
                product_id = EXCLUDED.product_id,
                scraped_at = EXCLUDED.scraped_at
        `;
        const values = [
            data.slug, 
            data.name, 
            data.selling_price, 
            data.mrp, 
            data.availability,
            data.product_id
        ];
        await client.query(query, values);
    } catch (err) {
        if (err.code === '42P10' || err.code === '42601' || err.message.includes('constraint') || err.message.includes('ON CONFLICT')) {
            // Backup Insert if ON CONFLICT logic fails due to lack of constraints
            const fallbackQuery = `
                INSERT INTO dbt_dsarkar.shopify_pricing (slug, product_name, selling_price, mrp, in_stock, product_id, scraped_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW())
            `;
            await client.query(fallbackQuery, [
                data.slug, 
                data.name, 
                data.selling_price, 
                data.mrp, 
                data.availability,
                data.product_id
            ]);
        } else {
            throw err;
        }
    } finally {
        client.release();
    }
};

const scrapeProduct = async (browser, slug) => {
    const page = await browser.newPage();
    try {
        const apiUrl = `https://www.chumbak.com/products/${slug}.js`;
        const response = await page.goto(apiUrl, { waitUntil: 'domcontentloaded' });
        
        let jsonData;
        try {
            const content = await page.evaluate(() => {
                return document.body.innerText || document.documentElement.innerText;
            });
            jsonData = JSON.parse(content);
        } catch (e) {
            console.error(`[${slug}] Failed to parse JSON from backend call: ${e.message}`);
            return null;
        }

        const product_id = jsonData.id;
        const name = jsonData.title;
        const selling_price = jsonData.price / 100;
        const mrp = jsonData.compare_at_price ? jsonData.compare_at_price / 100 : selling_price;
        const availability = jsonData.available; // map to in_stock

        console.log(`[Success] Scraped ${slug} | ID: ${product_id} | Name: ${name} | Price: ₹${selling_price} | MRP: ₹${mrp} | In Stock: ${availability}`);
        
        return {
            slug,
            product_id,
            name,
            selling_price,
            mrp,
            availability
        };
    } catch (error) {
        console.error(`[Error] Failed to scrape ${slug}:`, error.message);
        return null;
    } finally {
        await page.close();
    }
};

(async () => {
    console.log(`Fetching Chumbak slugs from database...`);
    let slugs;
    try {
        slugs = await getSlugsFromDb();
        console.log(`Found ${slugs.length} slugs to process.`);
    } catch (err) {
        console.error('Error fetching slugs from database:', err.message);
        await pool.end();
        return;
    }

    const browser = await puppeteer.launch({ 
        headless: 'new', 
        args: ['--no-sandbox', '--disable-setuid-sandbox'] 
    });
    
    let processed = 0;

    for (const slug of slugs) {
        console.log(`Processing: ${slug}`);
        const data = await scrapeProduct(browser, slug);
        if (data) {
            try {
                await insertScrapedData(data);
                processed++;
            } catch (dbErr) {
                console.error(`[DB Error] Failed to insert data for ${slug}:`, dbErr.message);
            }
        }
        
        // Optional polite delay between requests
        await new Promise(r => setTimeout(r, 1000));
    }

    await browser.close();
    await pool.end();
    
    console.log(`\nCrawling completed. Successfully processed and inserted ${processed} products into dbt_dsarkar.shopify_pricing.`);
})();
