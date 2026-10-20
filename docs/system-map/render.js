// Renders docs/system-map/<test-type>.png from index.html, one image per test type.
// Run from the repo root:
//   docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -v "$PWD/docs/system-map:/data" mcr.microsoft.com/playwright:v1.49.1-noble \
//     sh -c "cd /tmp && npm i -s playwright@1.49.1 >/dev/null && NODE_PATH=/tmp/node_modules node /data/render.js"
const { chromium } = require("playwright");

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1500, height: 1800 } });
  await page.goto(`file://${__dirname}/index.html`);
  const types = await page.evaluate(() => Object.entries(TYPES).map(([k, t]) => [k, !t.backends || t.backends.includes("aws") ? "aws" : t.backends[0]]));
  await page.mouse.move(1, 1); // keep hover state off the canvas
  for (const [type, backend] of types) {
    await page.evaluate(([t, b]) => { state.type = t; state.backend = b; state.selected = null; fillTabs(); render(); }, [type, backend]);
    await page.waitForTimeout(200);
    await page.evaluate(() => { state.hover = "*"; drawWires(); }); // draw every connection
    await page.screenshot({ path: `${__dirname}/${type}.png`, clip: await page.locator(".scroller").boundingBox() });
    console.log(`${type}.png (${backend})`);
  }
  await browser.close();
})();
