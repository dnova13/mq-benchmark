// Locust HTML 리포트를 PNG로 캡처한다.
// 사용: node load/locust/capture.js <report.html> <out.png>
// 필요: npm i -D playwright && npx playwright install chromium (또는 PLAYWRIGHT_PATH로 설치 위치 지정)
const { chromium } = require(process.env.PLAYWRIGHT_PATH || 'playwright');

(async () => {
  const [, , html, out] = process.argv;
  if (!html || !out) {
    console.error('사용: node capture.js <report.html> <out.png>');
    process.exit(1);
  }
  const browser = await chromium.launch(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {});
  const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });
  await page.goto('file://' + require('path').resolve(html));
  await page.waitForTimeout(2500); // 차트 렌더링 대기
  await page.screenshot({ path: out, fullPage: true });
  await browser.close();
})();
