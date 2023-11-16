CREATE TABLE Rating(RatingID SERIAL PRIMARY KEY,
                    Rating SMALLINT NOT NULL,
                    Meaning VARCHAR(20) NOT NULL
                    );

CREATE TABLE Department(DepartmentID SERIAL PRIMARY KEY,
                        Title VARCHAR(30) NOT NULL,
                        Floor VARCHAR(30) NOT NULL);

CREATE TABLE Exhibition(ExhibitionID CHAR(6) PRIMARY KEY, 
                        Title VARCHAR(50) NOT NULL,
                        Information VARCHAR(250) NOT NULL,
                        StartDate DATE DEFAULT CURRENT_DATE
                        CHECK (StartDate <= CURRENT_DATE),
                        DepartmentID SMALLINT NOT NULL,
                        FOREIGN KEY (DepartmentID) REFERENCES Department(DepartmentID)
                        );

CREATE TABLE Vote(VoteID SERIAL PRIMARY KEY,
                    ExhibitionID CHAR(6) NOT NULL,
                    VoteTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    CHECK (VoteTime <= CURRENT_TIMESTAMP), 
                    RatingID smallint NOT NULL,
                    FOREIGN KEY (ExhibitionID) REFERENCES Exhibition(ExhibitionID),
                    FOREIGN KEY (RatingID) REFERENCES Rating(RatingID)
                    );

CREATE TABLE Assistance(AssistanceID SERIAL PRIMARY KEY,
                        ExhibitionID CHAR(6) NOT NULL,
                        AssistanceTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        CHECK (AssistanceTime <= CURRENT_TIMESTAMP),
                        FOREIGN KEY (ExhibitionID) REFERENCES Exhibition(ExhibitionID)
                        );

CREATE TABLE Emergency(EmergencyID SERIAL PRIMARY KEY, 
                        ExhibitionID CHAR(6) NOT NULL,
                        EmergencyTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        CHECK (EmergencyTime <= CURRENT_TIMESTAMP),
                        FOREIGN KEY (ExhibitionID) REFERENCES Exhibition(ExhibitionID)
                        );





