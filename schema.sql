CREATE TABLE IF NOT EXISTS repositories (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    full_name VARCHAR(500) NOT NULL,
    owner_login VARCHAR(255) NOT NULL,
    star_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(id)
);

CREATE INDEX idx_repositories_star_count ON repositories(star_count);
CREATE INDEX idx_repositories_owner ON repositories(owner_login);
CREATE INDEX idx_repositories_crawled_at ON repositories(crawled_at);
