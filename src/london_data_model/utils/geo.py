"""Geographic coordinate utilities."""

import math
from typing import Optional, Tuple


def bng_to_wgs84(easting: float, northing: float) -> Tuple[float, float]:
    """Convert British National Grid (OSGB36) easting/northing to WGS84 lat/lon.

    Based on Ordnance Survey "A Guide to coordinate systems in Great Britain" (v2.3).
    Returns (latitude, longitude) in decimal degrees.
    """
    lat_osgb, lon_osgb = _bng_to_osgb36(easting, northing)
    lat_wgs84, lon_wgs84 = _helmert_osgb36_to_wgs84(lat_osgb, lon_osgb)
    return math.degrees(lat_wgs84), math.degrees(lon_wgs84)


def parse_bng_coordinate(value: object) -> Optional[float]:
    """Parse an easting or northing value from a raw CSV field. Returns None on failure."""
    try:
        parsed = float(str(value).strip())
        return parsed if parsed != 0.0 else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Airy 1830 ellipsoid (OSGB36)
_AIRY_A = 6377563.396
_AIRY_B = 6356256.909
_AIRY_E2 = 1.0 - (_AIRY_B / _AIRY_A) ** 2

# National Grid projection constants
_F0 = 0.9996012717
_PHI0 = math.radians(49.0)
_LAM0 = math.radians(-2.0)
_N0 = -100000.0
_E0 = 400000.0
_N_PARAM = (_AIRY_A - _AIRY_B) / (_AIRY_A + _AIRY_B)

# WGS84 ellipsoid
_WGS84_A = 6378137.000
_WGS84_B = 6356752.3141
_WGS84_E2 = 1.0 - (_WGS84_B / _WGS84_A) ** 2

# Helmert transformation parameters: OSGB36 → WGS84
_TX = 446.448
_TY = -125.157
_TZ = 542.060
_RX = math.radians(0.1502 / 3600.0)
_RY = math.radians(0.2470 / 3600.0)
_RZ = math.radians(0.8421 / 3600.0)
_S = -20.4894e-6


def _meridian_arc(phi0: float, phi: float) -> float:
    n = _N_PARAM
    return _AIRY_B * _F0 * (
        (1 + n + 1.25 * n ** 2 + 1.25 * n ** 3) * (phi - phi0)
        - (3 * n + 3 * n ** 2 + 2.625 * n ** 3) * math.sin(phi - phi0) * math.cos(phi + phi0)
        + (1.875 * n ** 2 + 1.875 * n ** 3) * math.sin(2 * (phi - phi0)) * math.cos(2 * (phi + phi0))
        - (35 / 24) * n ** 3 * math.sin(3 * (phi - phi0)) * math.cos(3 * (phi + phi0))
    )


def _bng_to_osgb36(easting: float, northing: float) -> Tuple[float, float]:
    """Transverse Mercator inverse projection: (E, N) → (φ, λ) in OSGB36."""
    phi = _PHI0 + (northing - _N0) / (_AIRY_A * _F0)

    for _ in range(100):
        M = _meridian_arc(_PHI0, phi)
        delta = northing - _N0 - M
        phi += delta / (_AIRY_A * _F0)
        if abs(delta) < 1e-5:
            break

    sin_phi = math.sin(phi)
    cos_phi = math.cos(phi)
    tan_phi = math.tan(phi)

    nu = _AIRY_A * _F0 / math.sqrt(1 - _AIRY_E2 * sin_phi ** 2)
    rho = _AIRY_A * _F0 * (1 - _AIRY_E2) / (1 - _AIRY_E2 * sin_phi ** 2) ** 1.5
    eta2 = nu / rho - 1.0

    dE = easting - _E0
    tan2 = tan_phi ** 2
    tan4 = tan_phi ** 4

    VII = tan_phi / (2 * rho * nu)
    VIII = tan_phi / (24 * rho * nu ** 3) * (5 + 3 * tan2 + eta2 - 9 * tan2 * eta2)
    IX = tan_phi / (720 * rho * nu ** 5) * (61 + 90 * tan2 + 45 * tan4)

    sec_phi = 1.0 / cos_phi
    X = sec_phi / nu
    XI = sec_phi / (6 * nu ** 3) * (nu / rho + 2 * tan2)
    XII = sec_phi / (120 * nu ** 5) * (5 + 28 * tan2 + 24 * tan4)
    XIIA = sec_phi / (5040 * nu ** 7) * (61 + 662 * tan2 + 1320 * tan4 + 720 * tan_phi ** 6)

    phi_out = phi - VII * dE ** 2 + VIII * dE ** 4 - IX * dE ** 6
    lam_out = _LAM0 + X * dE - XI * dE ** 3 + XII * dE ** 5 - XIIA * dE ** 7

    return phi_out, lam_out


def _helmert_osgb36_to_wgs84(phi: float, lam: float) -> Tuple[float, float]:
    """Apply Helmert 7-parameter transformation from OSGB36 to WGS84."""
    sin_phi = math.sin(phi)
    cos_phi = math.cos(phi)

    nu = _AIRY_A / math.sqrt(1 - _AIRY_E2 * sin_phi ** 2)
    x = nu * cos_phi * math.cos(lam)
    y = nu * cos_phi * math.sin(lam)
    z = nu * (1 - _AIRY_E2) * sin_phi

    x2 = _TX + (1 + _S) * (x - _RZ * y + _RY * z)
    y2 = _TY + (1 + _S) * (_RZ * x + y - _RX * z)
    z2 = _TZ + (1 + _S) * (-_RY * x + _RX * y + z)

    p = math.sqrt(x2 ** 2 + y2 ** 2)
    phi_new = math.atan2(z2, p * (1 - _WGS84_E2))

    for _ in range(10):
        nu2 = _WGS84_A / math.sqrt(1 - _WGS84_E2 * math.sin(phi_new) ** 2)
        phi_prev = phi_new
        phi_new = math.atan2(z2 + _WGS84_E2 * nu2 * math.sin(phi_new), p)
        if abs(phi_new - phi_prev) < 1e-12:
            break

    lam_new = math.atan2(y2, x2)
    return phi_new, lam_new
