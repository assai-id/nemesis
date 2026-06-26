import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

window['AuditMap'] = (() => {
  const SOURCE = 'audit-areas';
  const FILL_LAYER = 'audit-fill';
  const LINE_LAYER = 'audit-line';
  const HOVER_FILL = 'audit-fill-hover';
  const HOVER_LINE = 'audit-line-hover';

  let map = null;
  let popup = null;
  let hoveredId = null;
  let _isProvinceView = false;
  let _onAreaClick = null;
  let _getPopupHtml = null;

  let pinned = false;
  let pinnedAreaKey = null;
  let pinnedLngLat = null;

  function getFeatureAreaKey(props) {
    return _isProvinceView ? props.provinceKey : props.regionKey;
  }

  function buildStyledGeo(geo, getFeatureStyle) {
    return {
      type: 'FeatureCollection',
      features: geo.features.map((f) => ({
        type: 'Feature',
        geometry: f.geometry,
        properties: { ...f.properties, ...getFeatureStyle(f) },
      })),
    };
  }

  function walkRing(ring, fn) {
    for (const p of ring) fn(p[0], p[1]);
  }

  function walkPoly(poly, fn) {
    for (const ring of poly) walkRing(ring, fn);
  }

  function walkCoords(geometry, fn) {
    const c = geometry.coordinates;
    if (geometry.type === 'Point') {
      fn(c[0], c[1]);
    } else if (geometry.type === 'LineString' || geometry.type === 'MultiPoint') {
      for (const p of c) fn(p[0], p[1]);
    } else if (geometry.type === 'Polygon' || geometry.type === 'MultiLineString') {
      for (const ring of c) walkRing(ring, fn);
    } else if (geometry.type === 'MultiPolygon') {
      for (const poly of c) walkPoly(poly, fn);
    }
  }

  function computeBounds(geo) {
    let minLng = Infinity,
      minLat = Infinity,
      maxLng = -Infinity,
      maxLat = -Infinity;
    let hasCoords = false;
    geo.features.forEach((f) => {
      if (!f.geometry) return;
      walkCoords(f.geometry, (lng, lat) => {
        hasCoords = true;
        if (lng < minLng) minLng = lng;
        if (lat < minLat) minLat = lat;
        if (lng > maxLng) maxLng = lng;
        if (lat > maxLat) maxLat = lat;
      });
    });
    return hasCoords
      ? [
          [minLng, minLat],
          [maxLng, maxLat],
        ]
      : null;
  }

  function ensureMap(container) {
    if (map) return;
    map = new maplibregl.Map({
      container,
      center: [118, -2.5],
      zoom: 5,
      minZoom: 4,
      maxZoom: 12,
      style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
    });
  }

  function closePopup() {
    if (popup) {
      popup.remove();
      popup = null;
    }
  }

  function clearHover() {
    if (hoveredId !== null) {
      try {
        map.setFeatureState({ source: SOURCE, id: hoveredId }, { hover: false });
      } catch (e) {
        console.warn('Failed to clear hover state:', e);
      }
      hoveredId = null;
    }
  }

  function showPopupAt(lngLat, html) {
    if (!popup) {
      popup = new maplibregl.Popup({
        closeButton: false,
        closeOnClick: false,
        maxWidth: '320px',
        className: 'audit-popup',
        offset: 12,
      });
    }
    popup.setLngLat(lngLat).setHTML(html).addTo(map);
  }

  function pinPopup(areaKey, lngLat) {
    pinned = true;
    pinnedAreaKey = areaKey;
    pinnedLngLat = lngLat;
    if (_getPopupHtml) {
      const html = _getPopupHtml(areaKey);
      if (html) showPopupAt(lngLat, html);
    }
  }

  function unpinPopup() {
    pinned = false;
    pinnedAreaKey = null;
    pinnedLngLat = null;
    closePopup();
  }

  function restorePinnedPopup() {
    if (pinned && pinnedAreaKey && _getPopupHtml) {
      const html = _getPopupHtml(pinnedAreaKey);
      if (html) showPopupAt(pinnedLngLat, html);
    }
  }

  function addLayers() {
    map.addSource(SOURCE, {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
      generateId: true,
    });

    map.addLayer({
      id: FILL_LAYER,
      type: 'fill',
      source: SOURCE,
      paint: {
        'fill-color': ['coalesce', ['get', 'fillColor'], '#243155'],
        'fill-opacity': ['coalesce', ['get', 'fillOpacity'], 0.08],
      },
    });

    map.addLayer({
      id: LINE_LAYER,
      type: 'line',
      source: SOURCE,
      paint: {
        'line-color': ['coalesce', ['get', 'strokeColor'], '#b5a882'],
        'line-width': ['coalesce', ['get', 'strokeWidth'], 0.8],
        'line-opacity': ['coalesce', ['get', 'strokeOpacity'], 0.17],
      },
    });

    // Hover highlight layers driven by feature-state
    map.addLayer({
      id: HOVER_FILL,
      type: 'fill',
      source: SOURCE,
      paint: {
        'fill-color': ['coalesce', ['get', 'fillColor'], '#243155'],
        'fill-opacity': [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          ['min', ['+', ['coalesce', ['get', 'fillOpacity'], 0.08], 0.16], 0.85],
          0,
        ],
      },
    });

    map.addLayer({
      id: HOVER_LINE,
      type: 'line',
      source: SOURCE,
      paint: {
        'line-color': '#f0d8a8',
        'line-width': 1.8,
        'line-opacity': ['case', ['boolean', ['feature-state', 'hover'], false], 1, 0],
      },
    });

    map.on('mousemove', FILL_LAYER, (e) => {
      if (!e.features.length) return;

      map.getCanvas().style.cursor = 'pointer';
      const feature = e.features[0];
      const id = feature.id;

      if (hoveredId !== null && hoveredId !== id) {
        map.setFeatureState({ source: SOURCE, id: hoveredId }, { hover: false });
      }
      hoveredId = id;
      map.setFeatureState({ source: SOURCE, id: id }, { hover: true });

      if (_getPopupHtml && feature.properties) {
        const areaKey = getFeatureAreaKey(feature.properties);
        unpinPopup();
        const html = _getPopupHtml(areaKey);
        if (html) showPopupAt(e.lngLat, html);
      }
    });

    map.on('mouseleave', FILL_LAYER, () => {
      map.getCanvas().style.cursor = '';
      clearHover();
      if (!pinned) closePopup();
    });

    map.on('click', FILL_LAYER, (e) => {
      if (!e.features.length) return;
      const feature = e.features[0];
      const areaKey = getFeatureAreaKey(feature.properties);
      pinPopup(areaKey, e.lngLat);
      if (_onAreaClick) _onAreaClick(areaKey);
    });
  }

  function render(container, geo, options, onReady) {
    _isProvinceView = options.isProvinceView;
    _onAreaClick = options.onAreaClick;
    _getPopupHtml = options.getPopupHtml;

    ensureMap(container);

    const apply = () => {
      if (!map.getSource(SOURCE)) {
        addLayers();
      }

      clearHover();
      closePopup();

      const styledGeo = buildStyledGeo(geo, options.getFeatureStyle);
      map.getSource(SOURCE).setData(styledGeo);

      if (options.fitBounds) {
        const bounds = computeBounds(geo);
        if (bounds) {
          map.fitBounds(bounds, {
            padding: options.isProvinceView ? 80 : 50,
            duration: 300,
          });
        }
      }

      if (onReady) onReady();
    };

    if (map.isStyleLoaded()) {
      apply();
    } else {
      map.once('load', apply);
    }
  }

  function refresh(geo, getFeatureStyle) {
    if (!map?.getSource(SOURCE)) return;
    clearHover();
    closePopup();
    map.getSource(SOURCE).setData(buildStyledGeo(geo, getFeatureStyle));
    restorePinnedPopup();
  }

  return { render, refresh, closePopup, unpinPopup };
})();
