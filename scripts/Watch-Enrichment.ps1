<#
.SYNOPSIS
Affiche le suivi de collecte sans lancer ni modifier le collecteur.
.EXAMPLE
.\scripts\Watch-Enrichment.ps1
.EXAMPLE
.\scripts\Watch-Enrichment.ps1 -Once
#>
[CmdletBinding()]
param(
    [ValidatePattern('^[0-9]{8}T[0-9]{6}Z$')]
    [string]$RunId,
    [ValidateSet('full', 'pilot')]
    [string]$Scope = 'full',
    [ValidateRange(2, 3600)]
    [int]$IntervalSeconds = 10,
    [switch]$Once
)

$projectRoot = Split-Path -Parent $PSScriptRoot
if (-not $RunId) {
    $selectionPath = Join-Path $projectRoot 'data\enrichment\current_selection.json'
    try {
        $RunId = (Get-Content -LiteralPath $selectionPath -Raw -Encoding UTF8 -ErrorAction Stop |
            ConvertFrom-Json -ErrorAction Stop).run_id
    } catch {
        throw "Selection introuvable ou illisible. Indiquer -RunId YYYYMMDDTHHMMSSZ. $($_.Exception.Message)"
    }
    if ($RunId -notmatch '^[0-9]{8}T[0-9]{6}Z$') {
        throw 'Identifiant de selection invalide.'
    }
}
$progressPath = Join-Path $projectRoot "results\enrichment\$RunId\collection_$Scope.json"

function Get-CountValue($Counters, [string]$Name) {
    if ($null -eq $Counters) { return 0 }
    $property = $Counters.PSObject.Properties[$Name]
    if ($null -eq $property) { return 0 }
    return [long]$property.Value
}

do {
    $state = $null
    try {
        $state = Get-Content -LiteralPath $progressPath -Raw -Encoding UTF8 -ErrorAction Stop |
            ConvertFrom-Json -ErrorAction Stop
        if (-not $Once) { Clear-Host }
        $processed = [long]$state.processed
        $total = [long]$state.total_targets
        $percent = if ($total -gt 0) { 100.0 * $processed / $total } else { 0 }
        $filled = [int][Math]::Floor([Math]::Min(100, [Math]::Max(0, $percent)) / 2)
        $bar = ('#' * $filled) + ('-' * (50 - $filled))
        Write-Output "SUIVI ENRICHISSEMENT IMDb - $RunId ($Scope)"
        Write-Output ('Consultation : {0}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))
        Write-Output ('[{0}] {1:N2} %' -f $bar, $percent)
        Write-Output ('Cibles traitees : {0:N0} / {1:N0} | Restantes : {2:N0}' -f $processed, $total, [Math]::Max(0, $total - $processed))
        Write-Output ('Etat declare : {0} | Phase : {1}' -f $state.status, $state.phase)
        Write-Output ('Articles HTML recuperes (cache inclus) : {0:N0}' -f (Get-CountValue $state.overall_status_counts 'collected'))
        Write-Output ('Sans correspondance Wikidata : {0:N0}' -f (Get-CountValue $state.overall_status_counts 'not_found'))
        Write-Output ('Sans article anglais : {0:N0}' -f (Get-CountValue $state.overall_status_counts 'no_english_article'))
        Write-Output ('Correspondances ambigues : {0:N0}' -f (Get-CountValue $state.overall_status_counts 'ambiguous_match'))
        Write-Output ('Erreurs API : {0:N0}' -f (Get-CountValue $state.overall_status_counts 'api_error'))
        Write-Output ('Reponses reutilisees depuis la reprise : {0:N0}' -f (Get-CountValue $state.counts 'reused'))
        Write-Output ('Nouveaux articles depuis la reprise : {0:N0}' -f (Get-CountValue $state.counts 'collected'))
        $updated = [DateTimeOffset]::MinValue
        if ([DateTimeOffset]::TryParse([string]$state.updated_at_utc, [ref]$updated)) {
            $age = [Math]::Max(0, ([DateTimeOffset]::UtcNow - $updated.ToUniversalTime()).TotalSeconds)
            Write-Output ('Dernier point : {0} (il y a {1:N0} s)' -f $updated.ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss zzz'), $age)
            if ($state.status -eq 'running' -and $age -gt 300) {
                Write-Output 'ATTENTION : aucun point recent. Verifier le processus Docker ; ce fichier seul ne prouve pas son activite.'
            }
        } else {
            Write-Output 'Dernier point non date : activite du processus non verifiable depuis ce fichier.'
        }
        if ($state.error) { Write-Output ('Erreur signalee : {0}' -f $state.error) }
        Write-Output ''
        Write-Output 'Les compteurs sont enregistres par lots de 50 : un palier entre deux points est normal.'
        Write-Output 'Ce pourcentage mesure les demandes traitees, pas la couverture finale des textes nettoyes.'
        Write-Output 'Fermer ce suivi ou Ctrl+C ici ne coupe pas la collecte.'
        if ($state.status -eq 'completed') {
            Write-Output 'Parcours termine : verifier les erreurs API puis lancer la preparation Spark et le controle de couverture.'
        }
    } catch {
        if (-not $Once) { Clear-Host }
        Write-Output ('Point de progression indisponible : {0}' -f $_.Exception.Message)
        Write-Output ('Fichier attendu : {0}' -f $progressPath)
        if ($Once) { throw }
    }
    if ($Once -or ($null -ne $state -and $state.status -in @('completed', 'interrupted', 'failed'))) {
        break
    }
    Start-Sleep -Seconds $IntervalSeconds
} while ($true)
