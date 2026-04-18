import { useEffect, useState } from 'react'
import { MapContainer, TileLayer, GeoJSON, CircleMarker, Popup, useMap } from 'react-leaflet'
import type { PathOptions, Layer } from 'leaflet'
import 'leaflet/dist/leaflet.css'
import type { Zone } from '../lib/api'
import { riskColor, COLORS } from '../lib/utils'

// Singapore planning areas GeoJSON — fetched once at module level and cached
const SG_GEOJSON_URL = 'https://raw.githubusercontent.com/yinshanyang/singapore/master/maps/0-planning-areas.geojson'

const SG_BOUNDS: [[number, number], [number, number]] = [[1.16, 103.60], [1.48, 104.02]]

// Approximate zone centres used as fallback for circle-marker mode
const ZONE_COORDS: Record<string, [number, number]> = {
  'Ang Mo Kio': [1.3691, 103.8454], 'Bedok': [1.3236, 103.9273],
  'Bishan': [1.3526, 103.8352], 'Boon Lay': [1.3389, 103.7047],
  'Bukit Batok': [1.3590, 103.7637], 'Bukit Merah': [1.2819, 103.8239],
  'Bukit Panjang': [1.3774, 103.7719], 'Bukit Timah': [1.3294, 103.8021],
  'Central Water Catchment': [1.3747, 103.8128], 'Changi': [1.3644, 103.9915],
  'Choa Chu Kang': [1.3840, 103.7470], 'Clementi': [1.3162, 103.7649],
  'Downtown Core': [1.2789, 103.8536], 'Geylang': [1.3201, 103.8918],
  'Hougang': [1.3612, 103.8863], 'Jurong East': [1.3329, 103.7436],
  'Jurong West': [1.3404, 103.7090], 'Kallang': [1.3100, 103.8714],
  'Lim Chu Kang': [1.4241, 103.7172], 'Mandai': [1.4013, 103.8124],
  'Marina East': [1.2802, 103.8706], 'Marina South': [1.2699, 103.8631],
  'Marine Parade': [1.3014, 103.9072], 'Museum': [1.2967, 103.8482],
  'Newton': [1.3138, 103.8378], 'Novena': [1.3294, 103.8434],
  'Orchard': [1.3048, 103.8318], 'Outram': [1.2796, 103.8393],
  'Pasir Ris': [1.3721, 103.9494], 'Paya Lebar': [1.3180, 103.8929],
  'Pioneer': [1.3193, 103.6968], 'Punggol': [1.3984, 103.9072],
  'Queenstown': [1.2942, 103.8062], 'River Valley': [1.2904, 103.8318],
  'Rochor': [1.3040, 103.8560], 'Seletar': [1.4041, 103.8695],
  'Sembawang': [1.4491, 103.8185], 'Sengkang': [1.3868, 103.8914],
  'Serangoon': [1.3554, 103.8679], 'Singapore River': [1.2878, 103.8474],
  'Southern Islands': [1.2046, 103.8418], 'Straits View': [1.2657, 103.8593],
  'Sungei Kadut': [1.4123, 103.7574], 'Tampines': [1.3496, 103.9568],
  'Tanglin': [1.3059, 103.8145], 'Tengah': [1.3740, 103.7245],
  'Toa Payoh': [1.3343, 103.8563], 'Tuas': [1.3029, 103.6374],
  'Western Islands': [1.2053, 103.7578], 'Western Water Catchment': [1.3888, 103.7131],
  'Woodlands': [1.4370, 103.7862], 'Yishun': [1.4304, 103.8354],
}

function FitBounds() {
  const map = useMap()
  useEffect(() => { map.fitBounds(SG_BOUNDS, { padding: [10, 10] }) }, [map])
  return null
}

interface Props {
  zones: Zone[]
  selectedId: number | null
  onSelect: (id: number | null) => void
  mode?: 'risk' | 'blue'
}

// Build a name→zone lookup (title-cased for GeoJSON matching)
function buildLookup(zones: Zone[]): Map<string, Zone> {
  const m = new Map<string, Zone>()
  zones.forEach(z => {
    m.set(z.zone_name.toUpperCase(), z)
    m.set(z.zone_name, z)
  })
  return m
}

// Use the same categorical encoding everywhere: low / medium / high.
function scoreToFill(level: string): string {
  const c = riskColor(level)
  const opacity = level === 'high' ? 0.72 : level === 'medium' ? 0.58 : 0.42
  // Convert hex to rgba
  const r = parseInt(c.slice(1, 3), 16)
  const g = parseInt(c.slice(3, 5), 16)
  const b = parseInt(c.slice(5, 7), 16)
  return `rgba(${r},${g},${b},${opacity})`
}

export default function SingaporeMap({ zones, selectedId, onSelect, mode = 'risk' }: Props) {
  const [geoJson, setGeoJson] = useState<GeoJSON.FeatureCollection | null>(null)
  const [geoError, setGeoError] = useState(false)
  const lookup = buildLookup(zones)

  useEffect(() => {
    fetch(SG_GEOJSON_URL)
      .then(r => { if (!r.ok) throw new Error('fetch failed'); return r.json() })
      .then(data => setGeoJson(data))
      .catch(() => setGeoError(true))
  }, [])

  // Style each GeoJSON feature by matching zone name
  const styleFeature = (feature?: GeoJSON.Feature): PathOptions => {
    const name: string = feature?.properties?.Name ?? feature?.properties?.PLN_AREA_N ?? ''
    const zone = lookup.get(name) ?? lookup.get(
      // Try title-case conversion
      name.toLowerCase().split(' ').map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(' ')
    )
    if (!zone) return { fillColor: 'rgba(99,140,255,0.08)', color: 'rgba(99,140,255,0.20)', weight: 1, fillOpacity: 1 }
    const isSelected = selectedId === zone.zone_id

    if (mode === 'blue') {
      return {
        fillColor: isSelected ? COLORS.primary : 'rgba(79,142,247,0.25)',
        color: isSelected ? COLORS.primary : 'rgba(79,142,247,0.40)',
        weight: isSelected ? 2 : 1, fillOpacity: 1,
      }
    }
    return {
      fillColor: scoreToFill(zone.risk_level),
      color: isSelected ? '#fff' : riskColor(zone.risk_level),
      weight: isSelected ? 2 : 0.8, fillOpacity: 1,
    }
  }

  const onEachFeature = (feature: GeoJSON.Feature, layer: Layer) => {
    const name: string = feature?.properties?.Name ?? feature?.properties?.PLN_AREA_N ?? ''
    const zone = lookup.get(name) ?? lookup.get(
      name.toLowerCase().split(' ').map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(' ')
    )
    if (!zone) return
    const color = riskColor(zone.risk_level)
    layer.bindPopup(`
      <div style="font-family:Inter,sans-serif;min-width:140px">
        <div style="font-size:0.82rem;font-weight:600;color:#fff;margin-bottom:4px">${zone.zone_name}</div>
        <div style="font-size:0.68rem;color:rgba(255,255,255,0.50);margin-bottom:6px">${zone.region} · ${zone.risk_level.toUpperCase()}</div>
        <div style="display:flex;justify-content:space-between;font-size:0.70rem">
          <span style="color:rgba(255,255,255,0.45)">Risk Score</span>
          <span style="color:${color};font-weight:600">${zone.delay_risk_score.toFixed(3)}</span>
        </div>
      </div>
    `)
    layer.on('click', () => onSelect(selectedId === zone.zone_id ? null : zone.zone_id))
  }

  return (
    <MapContainer
      center={[1.3521, 103.8198]}
      zoom={11}
      style={{ width: '100%', height: '100%', borderRadius: 'inherit' }}
      zoomControl={true}
      attributionControl={false}
    >
      <TileLayer
        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        maxZoom={18}
      />
      <FitBounds />

      {/* Choropleth — GeoJSON polygons coloured by risk score */}
      {geoJson && (
        <GeoJSON
          key={`${selectedId}-${mode}`}
          data={geoJson}
          style={styleFeature}
          onEachFeature={onEachFeature}
        />
      )}

      {/* Fallback: circle markers when GeoJSON fails to load or while loading */}
      {(geoError || !geoJson) && zones.map(z => {
        const coords = ZONE_COORDS[z.zone_name]
        if (!coords) return null
        const isSelected = selectedId === z.zone_id
        const color = mode === 'blue' ? COLORS.primary : riskColor(z.risk_level)
        return (
          <CircleMarker
            key={z.zone_id}
            center={coords}
            radius={isSelected ? 10 : Math.max(5, z.delay_risk_score * 16)}
            pathOptions={{
              fillColor: color, fillOpacity: isSelected ? 0.95 : 0.65,
              color: isSelected ? '#fff' : color, weight: isSelected ? 2 : 1,
            }}
            eventHandlers={{ click: () => onSelect(isSelected ? null : z.zone_id) }}
          >
            <Popup>
              <div style={{ fontFamily: 'Inter, sans-serif', minWidth: 140 }}>
                <div style={{ fontSize: '0.80rem', fontWeight: 600, color: '#fff', marginBottom: 4 }}>{z.zone_name}</div>
                <div style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.50)' }}>{z.risk_level.toUpperCase()} · {z.delay_risk_score.toFixed(3)}</div>
              </div>
            </Popup>
          </CircleMarker>
        )
      })}
    </MapContainer>
  )
}
