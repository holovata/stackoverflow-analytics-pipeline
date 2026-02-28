-- Fact table: Questions
CREATE TABLE IF NOT EXISTS fact_questions (
    question_id INT PRIMARY KEY,
    title TEXT,
    body_markdown TEXT,
    creation_date TIMESTAMP,
    is_answered BOOLEAN,
    view_count INT,
    answer_count INT,
    score INT,
    owner_user_id INT,
    tags_raw JSONB,
    last_loaded_at TIMESTAMP DEFAULT NOW()
);

-- Bridge table (denormalized): Tags and Questions
CREATE TABLE IF NOT EXISTS bridge_question_tags (
    question_id INT,
    tag_name TEXT, -- text field to minimize number of lookups during insertion
    PRIMARY KEY (question_id, tag_name),
    FOREIGN KEY (question_id) REFERENCES fact_questions(question_id) ON DELETE CASCADE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_questions_date ON fact_questions(creation_date);
CREATE INDEX IF NOT EXISTS idx_tags_tagname ON bridge_question_tags(tag_name);