# SET %5DayVol

Top 20 SET and MAI index members (lists from set.or.th's SET and mai index pages) by **%5DayVol** = (Current Volume Today ÷ Average Volume of the Past 5 Days) × 100, i.e. today's volume (T) as a % of the average
volume of T-1 to T-5, the previous 5 trading days (200% = twice the average), for the Morning Session (13:00 Bangkok) and the Afternoon
Session (17:00, full-day volume at the close).

Live page: https://pridil.github.io/set-vol5d/

Data: TradingView daily bars (15-minute delayed feed). Built by `.github/workflows/pages.yml`
on GitHub Actions, started Mon–Fri 13:00 and 17:00 Bangkok by an external scheduler.
