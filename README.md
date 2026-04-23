# Triangle Scanner

Binance USDT-M 무기한선물에서 삼각수렴(대칭/상승/하강) 패턴을 감지해 **Discord**로 알림하는 봇. GitHub Actions에서 15분마다 자동 실행.

## 구성

- `scanner.py` — 1회 스캔 후 종료. 거래대금 상위 100개 심볼 · 15m 봉 기준.
- `.github/workflows/scan.yml` — 15분 cron + 수동 실행.
- `state.json` — 마지막 알림 시각 캐시 (같은 심볼 4시간 쿨다운). Actions cache로 영속화.

## 로컬 실행

```bash
pip install -r requirements.txt

# 웹훅 없이 콘솔 출력만 (감지 확인용)
python scanner.py

# 실제 Discord 전송
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..." python scanner.py
```

Windows PowerShell:
```powershell
$env:DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/..."
python scanner.py
```

## 주요 상수 (scanner.py 상단)

| 이름 | 값 | 의미 |
|---|---|---|
| `TIMEFRAME` | `15m` | 분석 봉 |
| `TOP_N` | 100 | 거래대금 상위 N개 |
| `MIN_R2` | 0.85 | 추세선 피팅 최소 R² |
| `APEX_MIN/MAX_BARS` | 5 / 50 | Apex까지 남은 봉 범위 |
| `MIN_COMPRESSION` | 0.30 | 최소 수렴률 |
| `ALERT_COOLDOWN` | 14400s | 같은 심볼 재알림 간격 |

## 배포 (GitHub Actions)

1. Public repo 생성 → 이 폴더 내용 push
2. Discord 채널에서 웹훅 생성 → URL 복사
3. Repo `Settings → Secrets and variables → Actions → New repository secret`
   - Name: `DISCORD_WEBHOOK_URL`
   - Value: 복사한 웹훅 URL
4. `Actions` 탭에서 **Triangle Scan** 워크플로우 활성화
5. `Run workflow`로 수동 실행해서 동작 확인
