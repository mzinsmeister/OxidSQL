
pub struct TableRef {
    table_id: u16
}

pub struct ColumnRef {
    table_ref: TableRef,
    column_id: u32
}

/*
    Catalog tables concept:
    General namespaces:
    (eventually it might be a good idea to have similar catalogs to postgres but for now 
    we won't enable runtime changing of types/access methods/aggregate functions, ...)

    Segment ids 0-1023: reserved for system tables, their fsis and indices
    Description of system tables (segment id n+1 is always reserved for the corresponding fsi):
    0: class (tables, indices, views, ...)
    2: attribute (table attributes)

    (that's it FOR NOW. Additional columns will be added over time)
 */

struct Catalog {
    cache: Arc<CatalogCache>,
}

struct CatalogCache {
    catalog_sp_segment: SlottedPageSegment
    // TODO: Agressively Cache stuff here
}

struct CatalogSegment {
    
}