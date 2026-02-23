$tokens = Get-Content "$env:USERPROFILE\.config\microsoft-graph\tokens.json" -Raw | ConvertFrom-Json
$headers = @{ Authorization = "Bearer $($tokens.access_token)" }

# Get the Navin lease email by date range
$filter = "receivedDateTime ge 2026-02-13T22:00:00Z and receivedDateTime le 2026-02-13T23:00:00Z and from/emailAddress/address eq 'navin@watsonsgroup.com.au'"
$url = "https://graph.microsoft.com/v1.0/me/messages?`$filter=$filter&`$select=subject,body,receivedDateTime"
$mail = Invoke-RestMethod -Uri $url -Headers $headers
foreach ($m in $mail.value) {
    Write-Output "=== $($m.subject) ==="
    $text = $m.body.content -replace '<[^>]+>', '' -replace '&nbsp;', ' ' -replace '&#\d+;', '' -replace '\s+', ' '
    Write-Output $text.Trim()
}
