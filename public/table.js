const getHeatmapColor = (value) => {
  const percentage = Math.abs(value) * 100;

  return `color-mix(in srgb, ${
    value > 0 ? "#84cc16" : "#fb7185"
  } ${percentage}%, transparent)`;
};

window.addEventListener("load", () => {
  const rows = document.querySelector("tbody").children;
  Array.from(rows).forEach((tr) => {
    const cells = tr.children;
    const cell = Array.from(cells)[3];
    if (cell instanceof HTMLTableCellElement) {
      const value = cell.dataset.value;

      if (value !== undefined) {
        cell.style.backgroundColor = getHeatmapColor(value);
        Array.from(cells)[2].style.backgroundColor = getHeatmapColor(value);
      }
    }
  });
});
