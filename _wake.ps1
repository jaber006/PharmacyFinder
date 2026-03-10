$params = '{"text":"Dashboard v4 leaderboard LIVE at localhost:8050","mode":"now"}'
Write-Host "Params: $params"
& clawdbot gateway call wake --params $params
