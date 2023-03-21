
struct Catalog {
    cache: Arc<CatalogCache>,
}

struct CatalogCache {
    catalog_sp_segment: SlottedPageSegment
    // TODO: Agressively Cache stuff here
}

struct CatalogSegment {
    
}