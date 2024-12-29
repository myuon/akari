const getHeatmapColor = (value) => {
  const normalized = parseFloat(value) / 100;
  const percentage = Math.abs(normalized) * 100;

  return `color-mix(in srgb, ${
    normalized > 0 ? "#84cc16" : "#fb7185"
  } ${percentage}%, transparent)`;
};

window.addEventListener("load", () => {
  const rows = document.querySelector("tbody").children;
  Array.from(rows).forEach((tr) => {
    const cells = tr.children;
    const cell = Array.from(cells)[4];
    if (cell instanceof HTMLTableCellElement) {
      const value = cell.dataset.value;

      if (value !== undefined) {
        cell.style.backgroundColor = getHeatmapColor(value);
      }
    }
  });
});
