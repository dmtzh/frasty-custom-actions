Push-Location $PSScriptRoot
Push-Location ..

try {
    deactivate
} catch {
}
& "./.venv/scripts/activate.ps1"
python -m pip freeze > ./requirements.txt
deactivate


$copy_folder_script = "./.deploy/copy_folder.py"
$publish_folder = "./.deploy/output"
$exclude_folders = @(".git", ".deploy", ".docker", ".venv", ".vscode", "__pycache__")

if (Test-Path $publish_folder) {
    Remove-Item -Path $publish_folder -Recurse
}

$publish_shared_folder = $publish_folder + "/shared"
$publish_infrastructure_folder = $publish_folder + "/infrastructure"

python $copy_folder_script . $publish_folder -e $exclude_folders
python $copy_folder_script ./../frasty/shared $publish_shared_folder -e $exclude_folders
python $copy_folder_script ./../frasty/infrastructure $publish_infrastructure_folder -e $exclude_folders
Copy-Item -Path $publish_folder/../Dockerfile -Destination $publish_folder

python -m venv $publish_folder/.venv
& "$publish_folder/.venv/Scripts/activate.ps1"
python -m pip install -r $publish_folder/requirements.txt
python -m pip install "faststream[cli]"
python -m pip freeze > $publish_folder/requirements.txt
deactivate
Remove-Item -Path $publish_folder/.venv -Recurse


Pop-Location
Pop-Location