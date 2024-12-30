const clamp = (value, min, max) => {
  return Math.min(Math.max(value, min), max);
};

const getHeatmapColor = (value_) => {
  const value = clamp(value_, -1, 1);
  const percentage = Math.abs(value) * 100;

  return `color-mix(in srgb, ${
    value > 0 ? "#84cc16" : "#fb7185"
  } ${percentage}%, transparent)`;
};

window.addEventListener("load", () => {
  const diffIndexes = [];
  const headers = document.querySelectorAll("thead th");
  Array.from(headers).forEach((th, i) => {
    if (th instanceof HTMLElement) {
      if (th.dataset.diff) {
        diffIndexes.push(i);
      }
    }
  });

  const rows = document.querySelector("tbody").children;
  Array.from(rows).forEach((tr) => {
    diffIndexes.forEach((index) => {
      const cells = tr.children;
      const cell = Array.from(cells)[index];
      if (cell instanceof HTMLTableCellElement) {
        const value = cell.dataset.value;

        if (value !== undefined) {
          cell.style.backgroundColor = getHeatmapColor(value);
          // diffのカラムの一つ前のカラムも色をつける
          Array.from(cells)[index - 1].style.backgroundColor =
            getHeatmapColor(value);
        }
      }
    });
  });
});
