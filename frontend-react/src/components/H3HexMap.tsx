/**
 * H3HexMap
 * --------
 * Renders H3 hex cell polygons on a Leaflet map.
 * Each cell is coloured by depletion_risk_score; clicking a cell selects it
 * and opens a tooltip with supply details.
 *
 * Boundaries arrive from the backend as [[lat, lon], ...] arrays. Leaflet's
 * GeoJSON layer expects [lon, lat] order, so we swap on conversion.
 */

import { useEffect, useMemo } from 'react'
import { MapContainer, TileLayer, GeoJSON, useMap } from 'react-leaflet'
import type { PathOptions, Layer } from 'leaflet'
import 'leaflet/dist/leaflet.css'
import type { H3HeatmapCell } from '../lib/api'
import { COLORS } from '../lib/utils'

const SG_BOUNDS: [[number, number], [number, number]] = [[1.16, 103.60], [1.48, 104.02]]

function FitBounds() {
  const map = useMap()
  useEffect(() => { map.fitBounds(SG_BOUNDS, { padding: [10, 10] }) }, [map])
  return null
}

function riskToColor(score: number): string {
  if (score >= 0.70) return COLORS.high
  if (score >= 0.40) return COLORS.medium
  return COLORS.low
}

function hexToRgba(hex: string, alpha: number): string {
  const r = parseInt(hex.slice(1, 3), 16)
  const g = parseInt(hex.slice(3, 5), 16)
  const b = parseInt(hex.slice(5, 7), 16)
  return `rgba(${r},${g},${b},${alpha})`
}

function cellsToGeoJson(cells: H3HeatmapCell[]): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: cells
      .filter(c => c.boundary && c.boundary.length >= 3)
      .map(cell => {
        // Convert [[lat, lon], ...] → [[lon, lat], ...] for GeoJSON
        const coords = cell.boundary.map(([lat, lon]) => [lon, lat] as [number, number])
        // Close the polygon ring if not already closed
        if (coords[0][0] !== coords[coords.length - 1][0] || coords[0][1] !== coords[coords.length - 1][1]) {
          coords.push(coords[0])
        }
        return {
          type: 'Feature' as const,
          geometry: { type: 'Polygon' as const, coordinates: [coords] },
          properties: { ...cell },
        }
      }),
  }
}

interface Props {
  cells: H3HeatmapCell[]
  selectedCell: string | null
  onSelect: (h3_cell: string | null) => void
}

export default function H3HexMap({ cells, selectedCell, onSelect }: Props) {
  const geoJson = useMemo(
    () => (cells.length > 0 ? cellsToGeoJson(cells) : null),
    [cells],
  )

  const styleFeature = (feature?: GeoJSON.Feature): PathOptions => {
    const props = feature?.properties as H3HeatmapCell | undefined
    if (!props) return { fillColor: 'transparent', color: 'transparent', weight: 0 }

    const color     = riskToColor(props.depletion_risk_score)
    const isSelected = selectedCell === props.h3_cell
    const baseAlpha  = props.depletion_risk_score >= 0.70 ? 0.72
      : props.depletion_risk_score >= 0.40 ? 0.52 : 0.32

    return {
      fillColor:   hexToRgba(color, isSelected ? 0.90 : baseAlpha),
      color:       isSelected ? '#ffffff' : color,
      weight:      isSelected ? 2 : 0.6,
      fillOpacity: 1,
    }
  }

  const onEachFeature = (feature: GeoJSON.Feature, layer: Layer) => {
    const p = feature.properties as H3HeatmapCell
    const color   = riskToColor(p.depletion_risk_score)
    const sparse  = p.sparse_cell_flag
      ? '<div style="margin-top:5px;padding:2px 7px;background:rgba(201,123,48,0.18);border:1px solid rgba(201,123,48,0.40);border-radius:4px;font-size:0.62rem;color:#C97B30">⚠ Sparse cell — treat as directional</div>'
      : ''

    layer.bindPopup(`
      <div style="font-family:Inter,sans-serif;min-width:170px">
        <div style="font-size:0.72rem;font-weight:600;color:#fff;margin-bottom:2px;word-break:break-all">${p.h3_cell}</div>
        <div style="font-size:0.62rem;color:rgba(255,255,255,0.45);margin-bottom:8px">${p.parent_zone} · ${p.severity_bucket.toUpperCase()}</div>
        <div style="display:flex;justify-content:space-between;font-size:0.68rem;margin-bottom:3px">
          <span style="color:rgba(255,255,255,0.45)">Depletion Risk</span>
          <span style="color:${color};font-weight:700">${p.depletion_risk_score.toFixed(3)}</span>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:0.68rem;margin-bottom:3px">
          <span style="color:rgba(255,255,255,0.45)">Predicted Shortage</span>
          <span style="color:rgba(255,255,255,0.80);font-weight:500">${p.predicted_shortage.toFixed(3)}</span>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:0.68rem;margin-bottom:3px">
          <span style="color:rgba(255,255,255,0.45)">Taxis</span>
          <span style="color:rgba(255,255,255,0.80)">${p.taxi_count}</span>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:0.68rem">
          <span style="color:rgba(255,255,255,0.45)">Baseline</span>
          <span style="color:rgba(255,255,255,0.80)">${p.baseline_supply.toFixed(1)}</span>
        </div>
        ${sparse}
      </div>
    `)
    layer.on('click', () => onSelect(selectedCell === p.h3_cell ? null : p.h3_cell))
  }

  return (
    <MapContainer
      center={[1.3521, 103.8198]}
      zoom={12}
      style={{ width: '100%', height: '100%', borderRadius: 'inherit' }}
      zoomControl={true}
      attributionControl={false}
    >
      <TileLayer
        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        maxZoom={18}
      />
      <FitBounds />

      {geoJson && (
        <GeoJSON
          key={`${selectedCell}-${cells.length}`}
          data={geoJson}
          style={styleFeature}
          onEachFeature={onEachFeature}
        />
      )}

      {cells.length === 0 && (
        // Invisible placeholder — empty-state shown outside the map
        <></>
      )}
    </MapContainer>
  )
}
