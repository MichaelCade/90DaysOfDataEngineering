(async function() {
    console.log("🚀 Initializing Clear-Target Run Ranges Harvester...");

    const mainRows = document.querySelectorAll('table tbody tr');
    const squad = [];

    mainRows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length < 6) return; 

        const nameLink = cells[1]?.querySelector('a');
        if (!nameLink) return;

        const playerName = nameLink.innerText.trim();
        if (!playerName || playerName === "Player" || playerName === "GAMES" || playerName.match(/^\d+\*?$/)) return;

        const idMatch = nameLink.href.match(/\/(\d+)\?/);
        if (idMatch) {
            const playerId = idMatch[1];
            squad.push({
                "Player": playerName,
                "Total Runs": parseInt(cells[5]?.innerText.trim()) || 0,
                "High Score": parseInt(cells[6]?.innerText.trim().replace('*','')) || 0,
                "Not Out": cells[6]?.innerText.trim().includes('*'),
                "Caught": parseInt(cells[13]?.innerText.trim()) || 0,
                "Bowled": parseInt(cells[12]?.innerText.trim()) || 0,
                "LBW": parseInt(cells[14]?.innerText.trim()) || 0,
                "Run Out": parseInt(cells[16]?.innerText.trim()) || 0,
                "url": `https://uffington.play-cricket.com/player_stats/batting/${playerId}?rule_type_id=179&sub_tab=batting_run_ranges&tab=batting_stats`,
                "0-9": 0, "10-19": 0, "20-29": 0, "30-39": 0, "40-49": 0, "50-59": 0,
                "60-69": 0, "70-79": 0, "80-89": 0, "90-99": 0, "100-149": 0, "150+": 0
            });
        }
    });

    let workerWindow = window.open("about:blank", "cricketScraperWorker", "width=1200,height=850");
    if (!workerWindow) {
        console.error("❌ Popup blocked! Please allow popups for this site and re-run.");
        return;
    }

    const targetRanges = ["0-9", "10-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-89", "90-99", "100-149", "150+"];

    for (let i = 0; i < squad.length; i++) {
        const player = squad[i];

        if (player["Total Runs"] === 0) {
            delete player.url;
            continue;
        }

        console.log(`⏳ [${i + 1}/${squad.length}] Fetching Run Ranges page for: ${player.Player}...`);
        workerWindow.location.href = player.url;

        let dataRow = null;
        let headers = [];
        let attempts = 0;
        const maxAttempts = 30;

        while (!dataRow && attempts < maxAttempts) {
            await new Promise(r => setTimeout(r, 500));
            attempts++;
            
            try {
                const doc = workerWindow.document;
                if (workerWindow.location.href === "about:blank") continue;

                const table = doc.querySelector('table');
                if (!table) continue;

                headers = Array.from(table.querySelectorAll('thead th')).map(h => h.innerText.trim());
                if (!headers.includes("0-9")) continue;

                const trs = Array.from(table.querySelectorAll('tbody tr'));
                dataRow = trs.find(r => {
                    const cell = r.querySelector('td, th');
                    return cell && cell.innerText.trim() === '2026';
                });
            } catch (e) { }
        }

        if (dataRow && headers.length > 0) {
            const cells = Array.from(dataRow.querySelectorAll('td, th')).map(c => c.innerText.trim());
            targetRanges.forEach(range => {
                const colIdx = headers.indexOf(range);
                if (colIdx !== -1 && cells[colIdx]) {
                    player[range] = parseInt(cells[colIdx]) || 0;
                }
            });
            console.log(`  ✅ Mapped ${player.Player} range fields.`);
        } else {
            console.warn(`  ❌ Timeout waiting for 2026 range row content to render for ${player.Player}.`);
        }

        delete player.url;
    }

    workerWindow.close();
    console.log("🏁 Compilation Complete!");
    console.table(squad);

    const dataBlob = new Blob([JSON.stringify(squad, null, 2)], { type: 'application/json' });
    const downloadLink = document.createElement('a');
    downloadLink.href = URL.createObjectURL(dataBlob);
    downloadLink.download = 'uffington_1st_xi_complete_ranges_2026.json';
    document.body.appendChild(downloadLink);
    downloadLink.click();
    document.body.removeChild(downloadLink);
})();