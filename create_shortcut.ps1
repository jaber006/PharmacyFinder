$ws = New-Object -ComObject WScript.Shell
$desktop = $ws.SpecialFolders("Desktop")
$shortcut = $ws.CreateShortcut("$desktop\PharmacyFinder.lnk")
$shortcut.TargetPath = "C:\Users\MJ\Documents\GitHub\PharmacyFinder\PharmacyFinder.bat"
$shortcut.WorkingDirectory = "C:\Users\MJ\Documents\GitHub\PharmacyFinder"
$shortcut.IconLocation = "C:\Windows\System32\shell32.dll,14"
$shortcut.Description = "PharmacyFinder Dashboard - Live Data"
$shortcut.Save()
Write-Host "Shortcut created on Desktop!"
