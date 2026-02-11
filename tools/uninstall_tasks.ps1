Unregister-ScheduledTask -TaskName "Superstonk DD Autopilot (6h-20m)" -Confirm:$false -ErrorAction SilentlyContinue
Unregister-ScheduledTask -TaskName "Superstonk DD Hub Refresh (Monthly-20m)" -Confirm:$false -ErrorAction SilentlyContinue
Write-Host "Tasks removed (if they existed)." -ForegroundColor Yellow
