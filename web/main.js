// ============================================================
// The Streamer Effect - main.js
// by Maximus Fernandez
//
// This file builds all four visualizations for the project's
// data story. Currently implemented: viz 1 (per-game timeline).
// Vizs 2-4 will be added as separate render functions invoked
// from the same data-load pipeline.
// ============================================================


// ---------- shared constants ----------

// Per-game color identity, used across all four visualizations so a
// reader can recognize a game by its color anywhere on the page. These
// values match d3.schemeSet2 indices and are also defined as CSS
// variables in style.css so SVG strokes and CSS borders agree.
const GAME_COLORS = {
  among_us:          '#66c2a5',
  fall_guys:         '#fc8d62',
  vampire_survivors: '#8da0cb',
  lethal_company:    '#e78ac3',
};

// Display names for the games. The CSV uses snake_case identifiers,
// but we never want to show those to the reader.
const GAME_LABELS = {
  among_us:          'Among Us',
  fall_guys:         'Fall Guys',
  vampire_survivors: 'Vampire Survivors',
  lethal_company:    'Lethal Company',
};

// Fixed iteration order for the games. Matches the order used
// in the editorial narrative: Among Us first as the chronological
// origin of the streamer-effect phenomenon, then Fall Guys,
// Vampire Survivors, Lethal Company.
const GAMES = ['among_us', 'fall_guys', 'vampire_survivors', 'lethal_company'];

// Default x-axis windows per game. Each window is roughly centered on
// the game's viral period and gives enough lead-in to show the
// pre-spike obscurity. The "Show full history" toggle overrides this
// with the full data range when checked.
const GAME_WINDOWS = {
  among_us:          ['2020-01-01', '2021-06-01'],
  fall_guys:         ['2020-04-01', '2021-12-01'],
  vampire_survivors: ['2021-09-01', '2023-06-01'],
  lethal_company:    ['2023-08-01', '2025-06-01'],
};


// ---------- data loading ----------

// We load all data files up front so each visualization function can
// just receive the relevant slice. Doing it once also avoids
// duplicate fetches when the user switches games or interactions.
async function loadAllData() {
  const [master, lag, growth, events] = await Promise.all([
    d3.csv('../data/clean/master.csv',          d3.autoType),
    d3.csv('../data/clean/lag_summary.csv',     d3.autoType),
    d3.csv('../data/clean/growth_summary.csv',  d3.autoType),
    d3.csv('../data/raw/streamer_events.csv',   d3.autoType),
  ]);
  return { master, lag, growth, events };
}


// ============================================================
// VIZ 1: per-game timeline
// ============================================================
//
// Three lines on a shared timeline: Twitch avg viewers, Steam
// players (avg or peak depending on the game; see lag_summary),
// and Google Trends score. All three are rescaled to 0-100 of
// each series' own peak so the timing relationships become the
// visual focus rather than absolute magnitudes.
//
// The reader picks one game at a time via a dropdown. A "Show
// full history" toggle expands the x-axis from the default
// per-game viral window to the entire available time range.
// Streamer event markers from streamer_events.csv are drawn on
// the timeline; clicking one populates a detail panel below.
// ============================================================

function buildViz1(allData) {

  // ---------- references to DOM controls and the chart container ----------
  const select       = document.getElementById('viz1-game');
  const fullHistory  = document.getElementById('viz1-fullhistory');
  const chartEl      = document.getElementById('viz1-chart');
  const eventPanel   = document.getElementById('viz1-event-panel');
  const eventClose   = document.getElementById('viz1-event-close');
  const eventDate    = document.getElementById('viz1-event-date');
  const eventStream  = document.getElementById('viz1-event-streamer');
  const eventText    = document.getElementById('viz1-event-text');
  const eventLink    = document.getElementById('viz1-event-link');

  // ---------- create persistent chart structure ----------
  // We create the SVG and a tooltip div once, then update them on each
  // render. This avoids the flicker that would come from clearing and
  // rebuilding the entire DOM tree every time the user changes games.
  // The bottom margin is generous (60px) to leave room for the
  // streamer event markers, which sit BELOW the x-axis date labels
  // rather than between the labels and the chart. The right margin is
  // small because the in-line series labels were replaced by a
  // collapsible legend overlay drawn on top of the plot area.
  const margin = { top: 30, right: 30, bottom: 60, left: 50 };
  const width  = 720;
  const height = 440;
  const innerW = width  - margin.left - margin.right;
  const innerH = height - margin.top  - margin.bottom;

  // Y-position for streamer event markers, expressed in inner-chart
  // coordinates. innerH is the x-axis baseline; +38 puts the markers
  // safely below the date tick labels, which sit at roughly +24.
  const eventMarkerY = innerH + 38;

  const svg = d3.select(chartEl)
    .append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  // Axis groups, tooltip, and series-line groups are created once and
  // reused so we just update their attributes on each render.
  const xAxisG = g.append('g').attr('class', 'axis x-axis')
    .attr('transform', `translate(0,${innerH})`);
  const yAxisG = g.append('g').attr('class', 'axis y-axis');

  // y-axis label sits at the top of the chart, sideways like in NYT
  // charts. Tells the reader "this is normalized 0-100, not raw."
  g.append('text')
    .attr('class', 'axis-label')
    .attr('x', 0).attr('y', -12)
    .attr('text-anchor', 'start')
    .style('font-family', 'JetBrains Mono, monospace')
    .style('font-size', '10px')
    .style('fill', '#8a8a8a')
    .style('text-transform', 'uppercase')
    .style('letter-spacing', '0.1em')
    .text('% of series peak');

  // Container groups for each series. Order matters for z-stacking:
  // Trends behind Steam behind Twitch, since Twitch is the lead
  // narrative line.
  const trendsG = g.append('g').attr('class', 'series series-trends-g');
  const steamG  = g.append('g').attr('class', 'series series-steam-g');
  const twitchG = g.append('g').attr('class', 'series series-twitch-g');
  const eventsG = g.append('g').attr('class', 'events');

  // Hover guideline (vertical line that follows mouse).
  const hoverLine = g.append('line')
    .attr('class', 'hover-line')
    .attr('y1', 0).attr('y2', innerH)
    .style('opacity', 0);

  // Tooltip div, positioned absolutely inside the chart container.
  const tooltip = d3.select(chartEl).append('div').attr('class', 'tooltip');

  // Invisible overlay rect that captures mouse events for the
  // hover guideline + tooltip. Has to be on top of everything else
  // so it receives mouse events; we draw it last in render().
  const overlay = g.append('rect')
    .attr('class', 'overlay')
    .attr('width',  innerW)
    .attr('height', innerH)
    .style('fill', 'none')
    .style('pointer-events', 'all');


  // ---------- collapsible legend (top-right of the chart) ----------
  // The legend lives as an HTML element absolutely positioned over the
  // chart container. Built once here; only the swatch stroke colors
  // update on each render to match the active game's color.
  //
  // Each row is a single line: a tiny line-style swatch, then the
  // series name. The widget collapses to just the "Key" header by
  // default; clicking the caret expands it.
  const legendSel = d3.select(chartEl)
    .append('details')
    .attr('class', 'chart-legend');

  legendSel.append('summary')
    .attr('class', 'legend-summary')
    .html('Key <span class="legend-caret" aria-hidden="true">&#9662;</span>');

  // The three series and the dash pattern each one uses. The dash
  // values mirror the strokes drawn on the chart itself so the swatch
  // is a real preview rather than a generic indicator.
  const legendData = [
    { key: 'twitch', label: 'Twitch viewers', dash: '0' },
    { key: 'steam',  label: 'Steam players',  dash: '5,3' },
    { key: 'trends', label: 'Google Trends',  dash: '1,3' },
  ];

  legendData.forEach(item => {
    const row = legendSel.append('div').attr('class', 'legend-row');
    // Inline SVG swatch sized to render at exactly 24x8 px on screen
    // by setting both the viewBox and explicit width/height. Without
    // both, the SVG gets stretched by its parent flex container and
    // the line preview becomes a thick blob rather than a thin line.
    const swatch = row.append('svg')
      .attr('class', 'legend-swatch-svg')
      .attr('width', 24).attr('height', 8)
      .attr('viewBox', '0 0 24 8');
    swatch.append('line')
      .attr('class', `legend-swatch legend-swatch-${item.key}`)
      .attr('x1', 0).attr('x2', 24)
      .attr('y1', 4).attr('y2', 4)
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', item.dash);
    row.append('span').attr('class', 'legend-label').text(item.label);
  });


  // ---------- main render function ----------
  // Called whenever the game selection or the full-history toggle
  // changes. Pulls the per-game slice, rescales each series, and
  // updates all the chart elements in place.
  function render() {
    const game = select.value;

    // Filter master.csv to this game's rows. Drop rows entirely outside
    // the chosen x-window so series scaling is based only on visible
    // data; otherwise an early-2018 outlier would distort the 0-100
    // rescale.
    let rows = allData.master.filter(d => d.game === game);
    rows.forEach(d => {
      // d3.autoType already converts month to a Date object.
      // Defensive: if it didn't, parse it here.
      if (!(d.month instanceof Date)) d.month = new Date(d.month);
    });
    rows.sort((a, b) => a.month - b.month);

    // Determine x-domain: per-game window by default, full data range
    // when the toggle is on.
    let xDomain;
    if (fullHistory.checked) {
      xDomain = d3.extent(rows, d => d.month);
    } else {
      const w = GAME_WINDOWS[game];
      xDomain = [new Date(w[0]), new Date(w[1])];
    }

    // Filter to the visible window for series rescaling.
    const visible = rows.filter(d => d.month >= xDomain[0] && d.month <= xDomain[1]);

    // Pick the Steam metric for this game. The lag_summary already
    // worked this out (avg_players or peak_players depending on
    // SteamDB tracking availability), so we just look it up.
    const lagRow = allData.lag.find(r => r.game === game);
    const steamCol = lagRow ? lagRow.steam_metric : 'peak_players';

    // Rescale each of the three series to 0-100 of its own peak,
    // computed over the visible window only. We also keep the raw
    // values around for the tooltip so the reader sees the actual
    // numbers, not the normalized ones.
    const peaks = {
      twitch: d3.max(visible, d => d.avg_viewers) || 1,
      steam:  d3.max(visible, d => d[steamCol])   || 1,
      trends: d3.max(visible, d => d.trends_score) || 1,
    };

    // Build the per-series datasets: one array of {month, value, raw}
    // per series. Skip points with null source values so the line
    // doesn't draw a phantom segment through missing months.
    const seriesData = {
      twitch: visible
        .filter(d => d.avg_viewers != null)
        .map(d => ({ month: d.month, value: (d.avg_viewers / peaks.twitch) * 100, raw: d.avg_viewers })),
      steam: visible
        .filter(d => d[steamCol] != null)
        .map(d => ({ month: d.month, value: (d[steamCol] / peaks.steam) * 100, raw: d[steamCol] })),
      trends: visible
        .filter(d => d.trends_score != null)
        .map(d => ({ month: d.month, value: (d.trends_score / peaks.trends) * 100, raw: d.trends_score })),
    };

    // ---------- scales ----------
    const x = d3.scaleTime().domain(xDomain).range([0, innerW]);
    const y = d3.scaleLinear().domain([0, 105]).range([innerH, 0]);

    // ---------- axes ----------
    const xAxis = d3.axisBottom(x)
      .ticks(6)
      .tickFormat(d3.timeFormat('%b %Y'));
    const yAxis = d3.axisLeft(y)
      .ticks(5)
      .tickFormat(d => d + '%');

    xAxisG.transition().duration(400).call(xAxis);
    yAxisG.transition().duration(400).call(yAxis);

    // Remove the dominating axis baseline lines so the chart looks
    // less boxed-in. Just keep the tick marks and labels.
    yAxisG.selectAll('.domain').remove();
    xAxisG.selectAll('.domain').attr('stroke', '#d8d4cc');

    // ---------- line generator ----------
    // monotoneX gives a smoothed-but-still-honest curve (passes
    // through every data point but interpolates between them in a
    // way that doesn't introduce false peaks).
    const lineGen = d3.line()
      .x(d => x(d.month))
      .y(d => y(d.value))
      .curve(d3.curveMonotoneX);

    const color = GAME_COLORS[game];

    // ---------- draw each series ----------
    function drawSeries(group, data, klass) {
      // Use D3's enter/update/exit pattern with a constant key so we
      // get smooth transitions when data changes between renders.
      const path = group.selectAll('path').data([data]);
      path.enter()
        .append('path')
        .attr('class', `series-line ${klass}`)
        .attr('stroke', color)
        .attr('d', lineGen)
        .merge(path)
        .transition().duration(400)
        .attr('stroke', color)
        .attr('d', lineGen);
      path.exit().remove();
    }

    drawSeries(twitchG, seriesData.twitch, 'series-twitch');
    drawSeries(steamG,  seriesData.steam,  'series-steam');
    drawSeries(trendsG, seriesData.trends, 'series-trends');

    // ---------- legend color update ----------
    // The collapsible legend's swatches use the per-game color, which
    // changes when the user switches games. We update those strokes
    // here so the legend stays in sync with the chart. The legend
    // structure itself is built once, outside render().
    legendSel.selectAll('.legend-swatch').attr('stroke', color);

    // ---------- streamer event markers ----------
    const gameEvents = allData.events
      .filter(e => e.game === game)
      .map(e => ({ ...e, date: new Date(e.date) }))
      .filter(e => e.date >= xDomain[0] && e.date <= xDomain[1]);

    const markers = eventsG.selectAll('circle.event-marker').data(gameEvents, d => d.date.toISOString() + d.streamer);
    markers.enter()
      .append('circle')
      .attr('class', 'event-marker')
      .attr('r', 5)
      .on('click', (evt, d) => showEvent(d))
      .merge(markers)
      .transition().duration(400)
      .attr('cx', d => x(d.date))
      .attr('cy', eventMarkerY); // below the x-axis date labels
    markers.exit().remove();

    // Overlay must be on top of the series and event markers. Re-raise
    // it on every render because new SVG nodes get appended above.
    overlay.raise();

    // ---------- hover behavior ----------
    // Single tooltip that shows the actual values (not normalized) for
    // whichever month the cursor is over. Uses a bisector to find the
    // closest month in the Twitch series, then looks up matching
    // values in the other two series.
    const bisect = d3.bisector(d => d.month).left;

    overlay
      .on('mousemove', function (evt) {
        const [mx] = d3.pointer(evt, this);
        const date = x.invert(mx);
        const i = bisect(seriesData.twitch, date);
        const point = seriesData.twitch[Math.min(i, seriesData.twitch.length - 1)];
        if (!point) return;
        const month = point.month;

        // Find matching points in the other two series. They may not
        // exist (a series can have nulls at certain months).
        const findAt = (arr) => arr.find(d => +d.month === +month);
        const sPoint = findAt(seriesData.steam);
        const tPoint = findAt(seriesData.trends);

        hoverLine
          .attr('x1', x(month))
          .attr('x2', x(month))
          .style('opacity', 0.5);

        const fmt = d3.format(',');
        tooltip.html(`
          <div class="tt-date">${d3.timeFormat('%B %Y')(month)}</div>
          <div class="tt-row"><span class="tt-label">Twitch viewers</span>
            <span class="tt-value">${point ? fmt(Math.round(point.raw)) : '-'}</span></div>
          <div class="tt-row"><span class="tt-label">Steam players</span>
            <span class="tt-value">${sPoint ? fmt(Math.round(sPoint.raw)) : '-'}</span></div>
          <div class="tt-row"><span class="tt-label">Trends score</span>
            <span class="tt-value">${tPoint ? Math.round(tPoint.raw) : '-'}</span></div>
        `);

        // Position tooltip relative to the chart container so it
        // follows the cursor without escaping the chart bounds. We
        // use offsetX/Y from the chart element to translate from
        // SVG coords to pixel coords on the page.
        const rect = chartEl.getBoundingClientRect();
        const tooltipX = evt.clientX - rect.left + 12;
        const tooltipY = evt.clientY - rect.top  + 12;
        tooltip
          .style('left', tooltipX + 'px')
          .style('top',  tooltipY + 'px')
          .classed('visible', true);
      })
      .on('mouseleave', () => {
        hoverLine.style('opacity', 0);
        tooltip.classed('visible', false);
      });
  }


  // ---------- streamer event detail panel ----------
  // When a marker is clicked, fill the panel below the chart with the
  // event's details and a source link, then unhide it. The panel
  // stays visible until the user closes it explicitly so they can
  // read it without it disappearing on cursor movement.
  function showEvent(d) {
    eventDate.textContent   = d3.timeFormat('%B %d, %Y')(d.date);
    eventStream.textContent = d.streamer;
    eventText.textContent   = d.event;
    eventLink.href          = d.url;
    eventPanel.hidden = false;
  }
  eventClose.addEventListener('click', () => {
    eventPanel.hidden = true;
  });


  // ---------- wire up controls and do initial render ----------
  select.addEventListener('change', () => {
    eventPanel.hidden = true; // close any open event when game changes
    render();
  });
  fullHistory.addEventListener('change', render);
  render();
}


// ============================================================
// VIZ 2: lag timeline (Gantt-style)
// ============================================================
//
// Horizontal Gantt-style chart with one row per game. The shared
// x-axis spans the entire 2018-2026 range so the reader sees not
// only the lag (Twitch-peak to Steam-peak) but also when each
// game's viral moment happened in the broader timeline. Among Us
// and Fall Guys both peaked in 2020; Vampire Survivors in early
// 2022; Lethal Company in late 2023.
//
// Within each row, two markers identify the Twitch peak and Steam
// peak months. A horizontal bar between them visualizes the lag.
// Because the lags are short (30-62 days) and the x-axis is long
// (8 years), the bars themselves render as only a few pixels wide,
// so we annotate each row with the lag in human-readable text
// ("31 days") to the right of its Steam-peak marker.
//
// Hovering a row reveals a tooltip with the full breakdown:
// Twitch peak month, Steam peak month, lag, and which Steam
// metric was used (avg_players or peak_players, since this
// varies by game per the merge_data design decision).
// ============================================================

function buildViz2(allData) {

  const chartEl = document.getElementById('viz2-chart');

  // ---------- chart geometry ----------
  // The chart is wider and shorter than viz 1 because it's a
  // calendar-spanning timeline rather than a series chart. The
  // bottom margin holds the x-axis, the top margin holds the
  // chart's inline legend, and the right margin is generous to
  // accommodate the "31 days" callout text that sits to the right
  // of each row's Steam-peak marker. The overall height is sized
  // so the chart visually fills its place in the article column
  // rather than leaving an awkward gap below.
  const margin = { top: 36, right: 110, bottom: 44, left: 140 };
  const width  = 720;
  const height = 380;
  const innerW = width  - margin.left - margin.right;
  const innerH = height - margin.top  - margin.bottom;

  const svg = d3.select(chartEl)
    .append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  // ---------- data preparation ----------
  // Convert string dates from lag_summary.csv to Date objects and
  // tag each row with its color and display name. Sorted by the
  // earlier of the two peaks so the rows display in viral-moment
  // chronological order (Among Us first, Lethal Company last).
  const lagRows = allData.lag.map(d => ({
    game: d.game,
    label: GAME_LABELS[d.game],
    color: GAME_COLORS[d.game],
    twitchPeak: new Date(d.twitch_peak_month),
    steamPeak:  new Date(d.steam_peak_month),
    lagDays:    d.lag_days,
    steamMetric: d.steam_metric,
  })).sort((a, b) => a.twitchPeak - b.twitchPeak);

  // ---------- scales ----------
  // X-axis: shared calendar from start of 2018 to mid-2026 so all
  // four games' peaks fit comfortably with breathing room on both
  // ends. The exact bounds are chosen to put Among Us's August 2020
  // peak nowhere near the left edge and Lethal Company's December
  // 2023 peak well clear of the right edge.
  const x = d3.scaleTime()
    .domain([new Date('2018-01-01'), new Date('2026-06-01')])
    .range([0, innerW]);

  // ---------- y-axis ----------
  // One band per game. Padding controls row spacing; reduced from
  // d3's default to allow more visual height per row given the
  // taller chart frame.
  const y = d3.scaleBand()
    .domain(lagRows.map(d => d.game))
    .range([0, innerH])
    .padding(0.35);

  // ---------- x-axis ----------
  g.append('g')
    .attr('class', 'axis x-axis')
    .attr('transform', `translate(0,${innerH})`)
    .call(d3.axisBottom(x).ticks(d3.timeYear.every(1)).tickFormat(d3.timeFormat('%Y')));

  // ---------- game labels on the left ----------
  // Drawn as plain text rather than a y-axis call because we want
  // them styled like editorial row headers, not axis tick labels.
  g.selectAll('text.game-label')
    .data(lagRows)
    .enter()
    .append('text')
    .attr('class', 'game-label')
    .attr('x', -12)
    .attr('y', d => y(d.game) + y.bandwidth() / 2)
    .attr('dy', '0.35em')
    .attr('text-anchor', 'end')
    .attr('fill', d => d.color)
    .text(d => d.label);

  // ---------- baseline rule per row ----------
  // A faint horizontal rule at each row's center makes the row
  // structure readable when the lag bar is too small to do so.
  g.selectAll('line.row-rule')
    .data(lagRows)
    .enter()
    .append('line')
    .attr('class', 'row-rule')
    .attr('x1', 0).attr('x2', innerW)
    .attr('y1', d => y(d.game) + y.bandwidth() / 2)
    .attr('y2', d => y(d.game) + y.bandwidth() / 2)
    .attr('stroke', 'var(--rule)')
    .attr('stroke-width', 0.5);

  // ---------- lag bars ----------
  // Drawn first (bottom layer) so the markers sit on top of them.
  // Each bar starts at the Twitch peak and ends at the Steam peak.
  // Filled with the game's color at low opacity to read as a soft
  // band rather than a solid block.
  g.selectAll('rect.lag-bar')
    .data(lagRows)
    .enter()
    .append('rect')
    .attr('class', 'lag-bar')
    .attr('x', d => x(d.twitchPeak))
    .attr('y', d => y(d.game) + y.bandwidth() / 2 - 6)
    .attr('width', d => Math.max(2, x(d.steamPeak) - x(d.twitchPeak)))
    .attr('height', 12)
    .attr('fill', d => d.color)
    .attr('opacity', 0.35);

  // ---------- Twitch peak markers ----------
  // Filled circles in the game's color. Twitch is the "lead"
  // event in this story (the cause), so it gets the more
  // prominent visual treatment.
  g.selectAll('circle.twitch-peak')
    .data(lagRows)
    .enter()
    .append('circle')
    .attr('class', 'peak-marker twitch-peak')
    .attr('cx', d => x(d.twitchPeak))
    .attr('cy', d => y(d.game) + y.bandwidth() / 2)
    .attr('r', 7)
    .attr('fill', d => d.color);

  // ---------- Steam peak markers ----------
  // Hollow circles (filled with page background, stroked in the
  // game's color) so they read as the "follow-up" event. Visually
  // distinguishes them from the Twitch markers without relying on
  // a separate color, since the per-game color identity is
  // consistent across all visualizations.
  g.selectAll('circle.steam-peak')
    .data(lagRows)
    .enter()
    .append('circle')
    .attr('class', 'peak-marker steam-peak')
    .attr('cx', d => x(d.steamPeak))
    .attr('cy', d => y(d.game) + y.bandwidth() / 2)
    .attr('r', 7)
    .attr('fill', 'var(--bg)')
    .attr('stroke', d => d.color)
    .attr('stroke-width', 2.5);

  // ---------- per-row lag callouts ----------
  // The lag bars themselves are only a handful of pixels wide on
  // an 8-year axis, so the actual lag value (in days) gets printed
  // in JetBrains Mono next to each row's Steam-peak marker. This
  // is what carries the "30-62 days" finding visually; the bars
  // are essentially decorative confirmation.
  g.selectAll('text.lag-callout')
    .data(lagRows)
    .enter()
    .append('text')
    .attr('class', 'lag-callout')
    .attr('x', d => x(d.steamPeak) + 14)
    .attr('y', d => y(d.game) + y.bandwidth() / 2)
    .attr('dy', '0.35em')
    .attr('fill', 'var(--ink)')
    .text(d => `${d.lagDays} days`);

  // ---------- legend at top of chart ----------
  // Two small swatches above the plot area, just inside the chart's
  // top margin. Tells the reader what the filled vs hollow circles
  // mean. Inline rather than as a collapsible widget because the
  // legend is small enough to live permanently without crowding.
  const legend = svg.append('g')
    .attr('class', 'viz2-legend')
    .attr('transform', `translate(${margin.left}, ${margin.top - 6})`);

  // Twitch peak swatch.
  legend.append('circle')
    .attr('cx', 0).attr('cy', 0).attr('r', 4)
    .attr('fill', 'var(--ink-soft)');
  legend.append('text')
    .attr('x', 8).attr('y', 0).attr('dy', '0.35em')
    .attr('class', 'legend-text')
    .text('Twitch peak');

  // Steam peak swatch.
  legend.append('circle')
    .attr('cx', 100).attr('cy', 0).attr('r', 4)
    .attr('fill', 'var(--bg)')
    .attr('stroke', 'var(--ink-soft)')
    .attr('stroke-width', 1.5);
  legend.append('text')
    .attr('x', 108).attr('y', 0).attr('dy', '0.35em')
    .attr('class', 'legend-text')
    .text('Steam peak');

  // ---------- hover tooltip ----------
  // Each row gets an invisible full-width hit rectangle so the
  // tooltip activates anywhere in the row, not just on the
  // markers (which are tiny). Same tooltip pattern as viz 1 for
  // visual consistency across the page.
  const tooltip = d3.select(chartEl).append('div').attr('class', 'tooltip');

  g.selectAll('rect.row-hit')
    .data(lagRows)
    .enter()
    .append('rect')
    .attr('class', 'row-hit')
    .attr('x', 0).attr('y', d => y(d.game))
    .attr('width', innerW).attr('height', y.bandwidth())
    .attr('fill', 'transparent')
    .style('pointer-events', 'all')
    .on('mousemove', function (evt, d) {
      const fmtDate = d3.timeFormat('%B %Y');
      // Friendly description of which Steam metric was used.
      // peak_players means SteamDB's all-time monthly concurrent
      // peak; avg_players is the monthly average. Tooltip
      // mentions which one to pre-empt confusion about why the
      // older games (Among Us, Fall Guys) used a different metric.
      const metricLabel = d.steamMetric === 'avg_players'
        ? 'avg concurrent'
        : 'peak concurrent';

      tooltip.html(`
        <div class="tt-date">${d.label}</div>
        <div class="tt-row"><span class="tt-label">Twitch peak</span>
          <span class="tt-value">${fmtDate(d.twitchPeak)}</span></div>
        <div class="tt-row"><span class="tt-label">Steam peak</span>
          <span class="tt-value">${fmtDate(d.steamPeak)}</span></div>
        <div class="tt-row"><span class="tt-label">Lag</span>
          <span class="tt-value">${d.lagDays} days</span></div>
        <div class="tt-row"><span class="tt-label">Steam metric</span>
          <span class="tt-value">${metricLabel}</span></div>
      `);

      const rect = chartEl.getBoundingClientRect();
      tooltip
        .style('left', (evt.clientX - rect.left + 12) + 'px')
        .style('top',  (evt.clientY - rect.top  + 12) + 'px')
        .classed('visible', true);
    })
    .on('mouseleave', () => {
      tooltip.classed('visible', false);
    });
}


// ============================================================
// VIZ 4: post-peak Steam player growth
// ============================================================
//
// Vertical bar chart with one bar per game on a logarithmic
// y-axis. Each bar's height is the percent growth in Steam
// player count in the month immediately following that game's
// Twitch viewership peak. The log scale is essential because
// the values span four orders of magnitude (177% for Lethal
// Company up to 423,750% for Vampire Survivors).
//
// The Fall Guys bar is special-cased: its growth is undefined
// because the Twitch peak was July 2020 and the game did not
// release on Steam until August 2020. Rather than dropping the
// bar (which would hide the most thesis-supporting data point
// of the whole project), we render it at the post-launch player
// count value with a dashed outline and a "pre-launch" label,
// and annotate it explicitly.
//
// Each bar carries a label above it with the absolute before
// and after player counts. This contextualizes the percentages:
// a 423,750% jump from 12 players to 50,862 reflects a different
// phenomenon than a 177% jump from 38,932 to 107,668, and the
// reader needs the raw numbers to see that.
// ============================================================

function buildViz4(allData) {

  const chartEl = document.getElementById('viz4-chart');

  // ---------- chart geometry ----------
  // Vertical-bar layout, sized to fill the article column. Top
  // margin is generous to accommodate the per-bar before/after
  // annotations that sit above each bar. Bottom margin holds the
  // x-axis with game labels. Left margin holds the y-axis tick
  // labels.
  const margin = { top: 80, right: 40, bottom: 50, left: 70 };
  const width  = 720;
  const height = 480;
  const innerW = width  - margin.left - margin.right;
  const innerH = height - margin.top  - margin.bottom;

  const svg = d3.select(chartEl)
    .append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  // ---------- data preparation ----------
  // Pull the four rows from growth_summary.csv. We carry around
  // the raw before/after player counts so each bar can show its
  // contextualizing annotation, and we keep the note column so
  // the Fall Guys "pre_launch_hype" case can be detected and
  // rendered differently.
  //
  // Sort order: by ascending percent growth, with the pre-launch
  // bar pinned to the leftmost position. This puts Fall Guys
  // first (the most thesis-supporting case), then ascending
  // growth Lethal Company -> Among Us -> Vampire Survivors so
  // the bars step up visually from left to right and the chart
  // reads as an escalating sequence.
  const growthRows = allData.growth.map(d => ({
    game: d.game,
    label: GAME_LABELS[d.game],
    color: GAME_COLORS[d.game],
    twitchPeakMonth: new Date(d.twitch_peak_month),
    steamMetric: d.steam_metric,
    playersAtPeak: d.players_at_peak,
    playersMonthAfter: d.players_month_after,
    pctGrowth: d.pct_growth,
    note: d.note,
    isPreLaunch: d.note === 'pre_launch_hype',
  }));

  // Custom sort: pre-launch bars first, then by ascending pct.
  growthRows.sort((a, b) => {
    if (a.isPreLaunch && !b.isPreLaunch) return -1;
    if (!a.isPreLaunch && b.isPreLaunch) return 1;
    return (a.pctGrowth || 0) - (b.pctGrowth || 0);
  });

  // ---------- scales ----------
  // X: one band per game.
  const x = d3.scaleBand()
    .domain(growthRows.map(d => d.game))
    .range([0, innerW])
    .padding(0.35);

  // Y: log scale. Domain runs from a sensible floor (10%) up to
  // just above the maximum observed value. Log scales cannot
  // include zero, which is fine because the smallest real growth
  // value here is 177%.
  //
  // The Fall Guys bar's "value" for plotting purposes is set to
  // a small multiple above the largest real growth value, so the
  // bar reads as taller than every other bar (consistent with its
  // status as the most extreme case) without dwarfing the chart
  // and crushing the other bars into invisibility. The actual
  // story is told by the "pre-launch" annotation and the "0 to
  // 172,213 players" context line, not by the bar's literal
  // height. The 1.5x multiplier keeps the bar visually dominant
  // while leaving comfortable vertical headroom for its
  // annotation to sit above it.
  const realGrowthMax = d3.max(
    growthRows.filter(d => !d.isPreLaunch),
    d => d.pctGrowth
  );
  const fallGuysVisualValue = realGrowthMax * 1.5;

  const valueFor = (d) => d.isPreLaunch ? fallGuysVisualValue : d.pctGrowth;

  const maxValue = d3.max(growthRows, valueFor);
  const y = d3.scaleLog()
    .domain([10, maxValue * 1.5])
    .range([innerH, 0])
    .clamp(true);

  // ---------- y-axis ----------
  // Custom tick values at log breakpoints (100%, 1K%, 10K%, 100K%,
  // 1M%). Labels are written in compact form to avoid clutter on
  // the axis (e.g., "1k%" not "1,000%").
  const yAxis = d3.axisLeft(y)
    .tickValues([100, 1000, 10000, 100000, 1000000])
    .tickFormat(d => {
      if (d >= 1000000) return (d / 1000000) + 'M%';
      if (d >= 1000)    return (d / 1000) + 'k%';
      return d + '%';
    });

  g.append('g')
    .attr('class', 'axis y-axis')
    .call(yAxis);

  // y-axis label, drawn well above the topmost tick so it does
  // not collide with the "1M%" tick text. Tells the reader what
  // the bars actually represent. Editorial convention is to label
  // axes inline rather than as rotated sidebar text.
  g.append('text')
    .attr('class', 'axis-label')
    .attr('x', -50).attr('y', -42)
    .attr('fill', 'var(--ink-faint)')
    .style('font-family', 'JetBrains Mono, monospace')
    .style('font-size', '10px')
    .style('text-transform', 'uppercase')
    .style('letter-spacing', '0.1em')
    .text('% growth, log scale');

  // ---------- x-axis ----------
  // Game labels sit below each bar in the game's color, in
  // Fraunces serif. We draw them manually rather than via
  // d3.axisBottom to get the typography right.
  g.append('line')
    .attr('class', 'x-axis-baseline')
    .attr('x1', 0).attr('x2', innerW)
    .attr('y1', innerH).attr('y2', innerH)
    .attr('stroke', 'var(--rule)');

  g.selectAll('text.x-game-label')
    .data(growthRows)
    .enter()
    .append('text')
    .attr('class', 'x-game-label')
    .attr('x', d => x(d.game) + x.bandwidth() / 2)
    .attr('y', innerH + 22)
    .attr('text-anchor', 'middle')
    .attr('fill', d => d.color)
    .text(d => d.label);

  // ---------- bars ----------
  // Pre-launch bar gets a dashed outline; others get solid fills.
  // Both use the per-game color identity for consistency.
  g.selectAll('rect.growth-bar')
    .data(growthRows)
    .enter()
    .append('rect')
    .attr('class', d => 'growth-bar' + (d.isPreLaunch ? ' pre-launch' : ''))
    .attr('x', d => x(d.game))
    .attr('y', d => y(valueFor(d)))
    .attr('width', x.bandwidth())
    .attr('height', d => innerH - y(valueFor(d)))
    .attr('fill', d => d.isPreLaunch ? 'transparent' : d.color)
    .attr('stroke', d => d.color)
    .attr('stroke-width', d => d.isPreLaunch ? 2 : 0)
    .attr('stroke-dasharray', d => d.isPreLaunch ? '6,4' : null)
    .attr('opacity', d => d.isPreLaunch ? 1 : 0.85);

  // ---------- per-bar annotations ----------
  // Two-line label above each bar: the percent (or "pre-launch"
  // for Fall Guys) on the first line, the before-and-after
  // player counts on the second line. Drawn as an SVG <text>
  // with a <tspan> for each line.
  const fmt = d3.format(',');

  // Two-line label group above each bar. The group origin is
  // positioned 32px above the top of the bar, which leaves room
  // for both lines (pct at y=0, context at y=14) to sit comfortably
  // clear of the bar without crowding into the chart's top margin.
  // Without this offset the context line would land directly on
  // the bar's top edge and become unreadable.
  const annot = g.selectAll('g.bar-annot')
    .data(growthRows)
    .enter()
    .append('g')
    .attr('class', 'bar-annot')
    .attr('transform', d => `translate(${x(d.game) + x.bandwidth() / 2}, ${y(valueFor(d)) - 32})`);

  // Top line: percent value or "pre-launch hype".
  annot.append('text')
    .attr('class', 'annot-pct')
    .attr('text-anchor', 'middle')
    .attr('fill', d => d.color)
    .text(d => {
      if (d.isPreLaunch) return 'pre-launch hype';
      // Format with comma separators when the value is large enough
      // to need them, otherwise just one decimal place.
      if (d.pctGrowth >= 10000) return '+' + fmt(Math.round(d.pctGrowth)) + '%';
      return '+' + d.pctGrowth.toFixed(0) + '%';
    });

  // Bottom line: before-and-after player counts in compact form.
  annot.append('text')
    .attr('class', 'annot-context')
    .attr('text-anchor', 'middle')
    .attr('y', 14)
    .attr('fill', 'var(--ink-soft)')
    .text(d => {
      if (d.isPreLaunch) {
        return `0 to ${fmt(d.playersMonthAfter)} players`;
      }
      return `${fmt(d.playersAtPeak)} to ${fmt(d.playersMonthAfter)} players`;
    });

  // ---------- hover tooltip ----------
  // Each bar has a hit area (the bar itself, since they are
  // already substantial). Hovering reveals a tooltip with the
  // full breakdown: game, peak month, metric used, before/after
  // values, and either the pct growth or the pre-launch note.
  const tooltip = d3.select(chartEl).append('div').attr('class', 'tooltip');

  g.selectAll('rect.growth-bar')
    .on('mousemove', function (evt, d) {
      const fmtDate = d3.timeFormat('%B %Y');
      const metricLabel = d.steamMetric === 'avg_players'
        ? 'avg concurrent'
        : 'peak concurrent';

      const growthLine = d.isPreLaunch
        ? `<div class="tt-row"><span class="tt-label">Status</span>
             <span class="tt-value">pre-launch (Steam)</span></div>`
        : `<div class="tt-row"><span class="tt-label">Growth</span>
             <span class="tt-value">+${fmt(Math.round(d.pctGrowth))}%</span></div>`;

      tooltip.html(`
        <div class="tt-date">${d.label}</div>
        <div class="tt-row"><span class="tt-label">Twitch peak</span>
          <span class="tt-value">${fmtDate(d.twitchPeakMonth)}</span></div>
        <div class="tt-row"><span class="tt-label">At peak</span>
          <span class="tt-value">${d.isPreLaunch ? 'not on Steam' : fmt(d.playersAtPeak) + ' players'}</span></div>
        <div class="tt-row"><span class="tt-label">Month after</span>
          <span class="tt-value">${fmt(d.playersMonthAfter)} players</span></div>
        ${growthLine}
        <div class="tt-row"><span class="tt-label">Steam metric</span>
          <span class="tt-value">${metricLabel}</span></div>
      `);

      const rect = chartEl.getBoundingClientRect();
      tooltip
        .style('left', (evt.clientX - rect.left + 12) + 'px')
        .style('top',  (evt.clientY - rect.top  + 12) + 'px')
        .classed('visible', true);
    })
    .on('mouseleave', () => {
      tooltip.classed('visible', false);
    });
}


// ============================================================
// VIZ 3: per-month scatter plot
// ============================================================
//
// One dot per (game, month) pair across the full dataset. X is
// monthly Twitch avg viewers, Y is monthly Steam peak players.
// Both axes are log scale because the values span four orders of
// magnitude (a few players up to hundreds of thousands).
//
// Color encodes the game so the reader can see whether all four
// games occupy the same trend or whether each game has its own
// trajectory. A trend line (least-squares fit in log-log space,
// which is equivalent to a power law in linear space) is overlaid
// to quantify the relationship. A toggle switches between a
// pooled trend (one line across all games) and a per-game trend
// (one line per game), letting the reader inspect both.
//
// Steam metric: peak_players for all games. Unlike the lag and
// growth visualizations, where we matched the metric to data
// availability per game, viz 3's purpose is validating the
// relationship across the maximum number of data points. Since
// peak_players is available for every (game, month) pair where
// the game existed on Steam, while avg_players is only available
// post-October-2022, peak gives us the full 90-month Among Us
// time series, the full Fall Guys series, etc. The per-month
// shape of the relationship is the same regardless of metric.
// ============================================================

// Helper: least-squares linear regression on (x, y) pairs.
// Returns { slope, intercept } such that y ≈ slope * x + intercept.
// We will call this on log-transformed values to fit a power law
// in the original linear space.
function linearRegression(points) {
  const n = points.length;
  if (n < 2) return null;
  let sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
  for (const [x, y] of points) {
    sumX += x;
    sumY += y;
    sumXY += x * y;
    sumXX += x * x;
  }
  const denom = n * sumXX - sumX * sumX;
  if (denom === 0) return null;
  const slope = (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;
  return { slope, intercept };
}

function buildViz3(allData) {

  const chartEl = document.getElementById('viz3-chart');
  const splitToggle = document.getElementById('viz3-split');

  // ---------- chart geometry ----------
  // Wider than tall but more square than the other vizs because
  // a scatter plot reads better with closer-to-equal axes; wide
  // skinny scatters compress the y-direction structure.
  const margin = { top: 30, right: 30, bottom: 60, left: 70 };
  const width  = 720;
  const height = 540;
  const innerW = width  - margin.left - margin.right;
  const innerH = height - margin.top  - margin.bottom;

  const svg = d3.select(chartEl)
    .append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  // ---------- data preparation ----------
  // Filter master.csv to (game, month) rows where both axes are
  // valid for plotting on log scales: positive Twitch avg viewers
  // and positive Steam peak players. Months with zeros or nulls
  // get dropped because log(0) is undefined.
  const points = allData.master
    .map(d => ({
      game: d.game,
      month: d.month,
      twitch: d.avg_viewers,
      steam: d.peak_players,
    }))
    .filter(d => d.twitch != null && d.twitch > 0 && d.steam != null && d.steam > 0);

  // ---------- scales ----------
  // Both log scales. The domains are computed from the data with
  // a touch of padding above and below so the points do not press
  // against the axis edges.
  const xExtent = d3.extent(points, d => d.twitch);
  const yExtent = d3.extent(points, d => d.steam);

  const x = d3.scaleLog()
    .domain([xExtent[0] * 0.7, xExtent[1] * 1.3])
    .range([0, innerW]);

  const y = d3.scaleLog()
    .domain([yExtent[0] * 0.7, yExtent[1] * 1.3])
    .range([innerH, 0]);

  // ---------- axes ----------
  // Custom tick formatter: powers-of-ten with commas. d3's default
  // log-scale tick text gets cluttered with intermediate values
  // like "2k" "5k" that we do not need.
  const fmtTick = (d) => {
    if (d >= 1000000) return d3.format(',')(Math.round(d / 1000000)) + 'M';
    if (d >= 1000)    return d3.format(',')(Math.round(d / 1000)) + 'k';
    return d3.format(',')(d);
  };

  g.append('g')
    .attr('class', 'axis x-axis')
    .attr('transform', `translate(0,${innerH})`)
    .call(d3.axisBottom(x).ticks(8, fmtTick));

  g.append('g')
    .attr('class', 'axis y-axis')
    .call(d3.axisLeft(y).ticks(6, fmtTick));

  // Axis labels.
  g.append('text')
    .attr('class', 'axis-label')
    .attr('x', innerW / 2).attr('y', innerH + 44)
    .attr('text-anchor', 'middle')
    .attr('fill', 'var(--ink-faint)')
    .style('font-family', 'JetBrains Mono, monospace')
    .style('font-size', '10px')
    .style('text-transform', 'uppercase')
    .style('letter-spacing', '0.1em')
    .text('Twitch avg viewers (log scale)');

  g.append('text')
    .attr('class', 'axis-label')
    .attr('transform', `translate(-50, ${innerH / 2}) rotate(-90)`)
    .attr('text-anchor', 'middle')
    .attr('fill', 'var(--ink-faint)')
    .style('font-family', 'JetBrains Mono, monospace')
    .style('font-size', '10px')
    .style('text-transform', 'uppercase')
    .style('letter-spacing', '0.1em')
    .text('Steam peak players (log scale)');

  // ---------- subtle log-grid ----------
  // Light horizontal and vertical rules at each major tick make
  // the log scale's compression structure readable without
  // dominating the chart.
  const gridG = g.append('g').attr('class', 'grid').lower();

  gridG.selectAll('line.x-grid')
    .data(x.ticks(8))
    .enter()
    .append('line')
    .attr('class', 'x-grid')
    .attr('x1', d => x(d)).attr('x2', d => x(d))
    .attr('y1', 0).attr('y2', innerH)
    .attr('stroke', 'var(--rule)')
    .attr('stroke-width', 0.5)
    .attr('opacity', 0.5);

  gridG.selectAll('line.y-grid')
    .data(y.ticks(6))
    .enter()
    .append('line')
    .attr('class', 'y-grid')
    .attr('x1', 0).attr('x2', innerW)
    .attr('y1', d => y(d)).attr('y2', d => y(d))
    .attr('stroke', 'var(--rule)')
    .attr('stroke-width', 0.5)
    .attr('opacity', 0.5);

  // ---------- scatter dots ----------
  // Drawn before the trend lines so the lines render on top and
  // remain readable through the scatter cloud.
  g.selectAll('circle.scatter-dot')
    .data(points)
    .enter()
    .append('circle')
    .attr('class', 'scatter-dot')
    .attr('cx', d => x(d.twitch))
    .attr('cy', d => y(d.steam))
    .attr('r', 3.5)
    .attr('fill', d => GAME_COLORS[d.game])
    .attr('opacity', 0.55)
    .attr('stroke', 'var(--bg)')
    .attr('stroke-width', 0.5);

  // ---------- trend lines ----------
  // Group for trend-line paths so we can clear and redraw them
  // when the user toggles between pooled and per-game modes.
  const trendG = g.append('g').attr('class', 'trends');

  // Helper: given a set of points and a color, fit a linear
  // regression in log-log space and append a path to trendG.
  // Mathematically this fits a power law (y = a * x^b) since
  // log(y) = log(a) + b * log(x). For our purposes that is the
  // right model: when streaming attention scales by a factor,
  // we expect player counts to scale by a related factor.
  // Returns {slope, r2} so the caller can display fit statistics.
  function drawTrend(subset, color, dashArray = null) {
    if (subset.length < 2) return null;
    const logged = subset.map(d => [Math.log10(d.twitch), Math.log10(d.steam)]);
    const fit = linearRegression(logged);
    if (!fit) return null;

    // Compute R-squared from the regression: 1 minus the ratio of
    // the residual sum of squares to the total sum of squares.
    // Tells the reader how tightly the points cluster around the
    // fitted line (1.0 = perfect fit, 0.0 = no relationship).
    const meanY = d3.mean(logged, p => p[1]);
    let ssRes = 0, ssTot = 0;
    for (const [lx, ly] of logged) {
      const yHat = fit.slope * lx + fit.intercept;
      ssRes += (ly - yHat) ** 2;
      ssTot += (ly - meanY) ** 2;
    }
    const r2 = ssTot > 0 ? 1 - (ssRes / ssTot) : 0;

    // Draw the trend line by sampling two endpoints in log space
    // and converting back to linear coordinates. A line in
    // log-log space remains a line on the chart, so two
    // endpoints are sufficient.
    const xLogMin = d3.min(logged, p => p[0]);
    const xLogMax = d3.max(logged, p => p[0]);
    const yLogMin = fit.slope * xLogMin + fit.intercept;
    const yLogMax = fit.slope * xLogMax + fit.intercept;

    trendG.append('line')
      .attr('class', 'trend-line')
      .attr('x1', x(Math.pow(10, xLogMin)))
      .attr('x2', x(Math.pow(10, xLogMax)))
      .attr('y1', y(Math.pow(10, yLogMin)))
      .attr('y2', y(Math.pow(10, yLogMax)))
      .attr('stroke', color)
      .attr('stroke-width', 2)
      .attr('stroke-dasharray', dashArray)
      .attr('fill', 'none');

    return { slope: fit.slope, r2 };
  }

  // Group for the regression statistics text. Cleared and
  // repopulated on every renderTrends() call to match the
  // toggle state.
  const statsG = g.append('g').attr('class', 'fit-stats')
    .attr('transform', `translate(${innerW - 8}, 8)`);

  // Function that (re)draws the trend lines based on the toggle
  // state. Called once at startup and again whenever the user
  // toggles "Split trend line by game".
  function renderTrends() {
    trendG.selectAll('*').remove();
    statsG.selectAll('*').remove();

    if (splitToggle.checked) {
      // Per-game: one trend line per game in that game's color.
      // Stats listed game-by-game in the same color. The 16px row
      // spacing leaves clear breathing room between labels so
      // longer game names like "Vampire Survivors" cannot collide
      // with the row above or below. Each text gets dy='0.35em'
      // so the SVG text baseline sits at the row's y-coordinate
      // rather than below it (default text baselines hang below
      // the y-coordinate, which can crowd adjacent rows).
      GAMES.forEach((game, i) => {
        const subset = points.filter(p => p.game === game);
        const fit = drawTrend(subset, GAME_COLORS[game]);
        if (!fit) return;

        statsG.append('text')
          .attr('x', 0)
          .attr('y', i * 16)
          .attr('dy', '0.35em')
          .attr('text-anchor', 'end')
          .attr('class', 'fit-stats-text')
          .attr('fill', GAME_COLORS[game])
          .text(`${GAME_LABELS[game]}: R² = ${fit.r2.toFixed(2)}`);
      });
    } else {
      // Pooled: one trend line across all games, in the editorial
      // ink color so it reads as the dominant analytical line
      // rather than belonging to any one game.
      const fit = drawTrend(points, 'var(--ink)');
      if (!fit) return;
      statsG.append('text')
        .attr('x', 0).attr('y', 0)
        .attr('dy', '0.35em')
        .attr('text-anchor', 'end')
        .attr('class', 'fit-stats-text')
        .attr('fill', 'var(--ink)')
        .text(`pooled R² = ${fit.r2.toFixed(2)}, slope = ${fit.slope.toFixed(2)}`);
    }
  }
  renderTrends();
  splitToggle.addEventListener('change', renderTrends);

  // ---------- legend ----------
  // Game color reference, drawn inside the chart in the top-left
  // (where there are no points because the smallest x-value is
  // around 1 viewer). Inline like viz 2's legend rather than
  // collapsible like viz 1, because the four-game color mapping
  // is essential information for reading this chart and should
  // stay visible.
  const legend = g.append('g')
    .attr('class', 'viz3-legend')
    .attr('transform', `translate(8, 8)`);

  GAMES.forEach((game, i) => {
    const row = legend.append('g').attr('transform', `translate(0, ${i * 16})`);
    row.append('circle')
      .attr('cx', 5).attr('cy', 5).attr('r', 4)
      .attr('fill', GAME_COLORS[game]);
    row.append('text')
      .attr('x', 16).attr('y', 5).attr('dy', '0.35em')
      .attr('class', 'viz3-legend-text')
      .text(GAME_LABELS[game]);
  });

  // ---------- hover tooltip ----------
  // Each dot gets a tooltip when hovered. Same tooltip pattern
  // as the other vizs.
  const tooltip = d3.select(chartEl).append('div').attr('class', 'tooltip');

  const fmtNum = d3.format(',');

  g.selectAll('circle.scatter-dot')
    .on('mousemove', function (evt, d) {
      const monthDate = d.month instanceof Date ? d.month : new Date(d.month);
      tooltip.html(`
        <div class="tt-date">${GAME_LABELS[d.game]}, ${d3.timeFormat('%B %Y')(monthDate)}</div>
        <div class="tt-row"><span class="tt-label">Twitch viewers</span>
          <span class="tt-value">${fmtNum(Math.round(d.twitch))}</span></div>
        <div class="tt-row"><span class="tt-label">Steam players</span>
          <span class="tt-value">${fmtNum(Math.round(d.steam))}</span></div>
      `);

      const rect = chartEl.getBoundingClientRect();
      tooltip
        .style('left', (evt.clientX - rect.left + 12) + 'px')
        .style('top',  (evt.clientY - rect.top  + 12) + 'px')
        .classed('visible', true);

      // Highlight the hovered dot by raising it and enlarging.
      d3.select(this).attr('r', 6).attr('opacity', 1);
    })
    .on('mouseleave', function () {
      tooltip.classed('visible', false);
      d3.select(this).attr('r', 3.5).attr('opacity', 0.55);
    });
}


// ============================================================
// boot
// ============================================================

loadAllData()
  .then(data => {
    buildViz1(data);
    buildViz2(data);
    buildViz4(data);
    buildViz3(data);
  })
  .catch(err => {
    console.error('Failed to load data:', err);
    document.getElementById('viz1-chart').innerHTML =
      '<p style="color:#a33;font-family:JetBrains Mono,monospace;font-size:0.85rem;">' +
      'Could not load data. Make sure data/clean/ files exist and you are serving via http (not opening the HTML file directly).' +
      '</p>';
  });