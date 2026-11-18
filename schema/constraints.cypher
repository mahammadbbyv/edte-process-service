CREATE CONSTRAINT unique_system_urn IF NOT EXISTS FOR (n:System) REQUIRE n.urn IS UNIQUE;
CREATE CONSTRAINT unique_resource_urn IF NOT EXISTS FOR (n:Resource) REQUIRE n.urn IS UNIQUE;
CREATE CONSTRAINT unique_actor_urn IF NOT EXISTS FOR (n:Actor) REQUIRE n.urn IS UNIQUE;
CREATE CONSTRAINT unique_role_urn IF NOT EXISTS FOR (n:Role) REQUIRE n.urn IS UNIQUE;
CREATE CONSTRAINT unique_step_urn IF NOT EXISTS FOR (n:ProcessStep) REQUIRE n.urn IS UNIQUE;
CREATE CONSTRAINT unique_doc_urn IF NOT EXISTS FOR (n:Document) REQUIRE n.urn IS UNIQUE;
CREATE CONSTRAINT unique_chunk_urn IF NOT EXISTS FOR (n:DocumentChunk) REQUIRE n.urn IS UNIQUE;
CREATE INDEX idx_resource_type IF NOT EXISTS FOR (n:Resource) ON (n.type);
CREATE INDEX idx_step_timestamp IF NOT EXISTS FOR (n:ProcessStep) ON (n.timestamp);
