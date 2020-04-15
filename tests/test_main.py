from designatedlands import DesignatedLands


def test_init():
    DL = DesignatedLands()
    assert DL.sources[0]["designation"] == "park_national"
    assert DL.sources_supporting[0]["alias"] == "tiles_20k"
    assert DL.sources[0]["src"] == "designatedlands.src_01_park_national"


def test_intersect():
    DL = DesignatedLands()
    DL.intersect(
        "designatedlands.designatedlands",
        "designatedlands.bc_boundary_land",
        "designatedlands.intersect_test",
        ["092B044"],
    )
    assert "designatedlands.intersect_test" in DL.db.tables
    assert "TRIAL ISLANDS ECOLOGICAL RESERVE" in [
        r["source_name"] for r in DL.db["designatedlands.intersect_test"]
    ]
    DL.db["designatedlands.test_intersect"].drop()


def test_rasterize():
    DL = DesignatedLands()
    DL.rasterize()