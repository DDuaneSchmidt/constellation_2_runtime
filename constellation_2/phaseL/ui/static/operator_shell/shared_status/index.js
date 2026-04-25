export function semanticTone(name, semantics) {
  const color = semantics?.[name]?.color || "yellow";
  return `tone-${color}`;
}

export function statePill(label, semantic, semantics) {
  const tone = semanticTone(semantic, semantics);
  return `<span class="state-pill ${tone}">${label}</span>`;
}

export function markerPill(marker, semantics) {
  const tone = semanticTone(marker, semantics);
  const label = semantics?.[marker]?.label || marker;
  return `<span class="marker-pill ${tone}">${label}</span>`;
}
