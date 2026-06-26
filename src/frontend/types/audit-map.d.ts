import type { Feature, FeatureCollection } from 'geojson';

export interface FeatureStyle {
  fillColor: string;
  fillOpacity: number;
  strokeColor: string;
  strokeWidth: number;
  strokeOpacity: number;
}

export interface MapRenderOptions {
  getFeatureStyle: (feature: Feature) => FeatureStyle;
  getPopupHtml: (areaKey: string) => string | null;
  onAreaClick: (areaKey: string) => void;
  fitBounds: boolean;
  isProvinceView: boolean;
}

export interface AuditMapInterface {
  render(
    container: HTMLElement,
    geo: FeatureCollection,
    options: MapRenderOptions,
    onReady?: () => void
  ): void;
  refresh(geo: FeatureCollection, getFeatureStyle: (f: Feature) => FeatureStyle): void;
  closePopup(): void;
  unpinPopup(): void;
}

declare global {
  interface Window {
    AuditMap: AuditMapInterface;
    DASHBOARD_API_BASE_URL?: string;
  }
  var AuditMap: AuditMapInterface;
  var DASHBOARD_API_BASE_URL: string | undefined;
}
