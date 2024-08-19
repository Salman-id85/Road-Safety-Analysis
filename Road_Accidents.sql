create database Road;
use Road;

CREATE TABLE Accidents (
    accident_id INT,
    location_id INT,
    road_id INT,
    weather_id INT,
    accident_time DATETIME,
    severity VARCHAR(50),
    number_of_vehicles INT,
    number_of_injuries INT,
    number_of_deaths INT,
    PRIMARY KEY (accident_id, accident_time)
)
PARTITION BY RANGE (YEAR(accident_time)) (
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
INSERT INTO Accidents (accident_id, location_id, road_id, weather_id, accident_time, severity, number_of_vehicles, number_of_injuries, number_of_deaths)
VALUES
(1, 101, 201, 301, '2023-05-15 08:30:00', 'Moderate', 2, 1, 0),
(2, 102, 202, 302, '2023-06-10 14:45:00', 'Severe', 3, 2, 1),
(3, 103, 203, 303, '2024-03-18 09:00:00', 'Minor', 1, 0, 0),
(4, 104, 204, 304, '2024-07-22 17:10:00', 'Fatal', 4, 3, 2),
(5, 105, 205, 305, '2025-01-11 22:50:00', 'Severe', 2, 1, 1);

SELECT severity, COUNT(*) AS accident_count
FROM Accidents
GROUP BY severity
ORDER BY accident_count DESC;

SELECT YEAR(accident_time) AS year, COUNT(*) AS accident_count
FROM Accidents
GROUP BY year
ORDER BY year DESC;


SELECT location_id, COUNT(*) AS accident_count
FROM Accidents
GROUP BY location_id
ORDER BY accident_count DESC;

CREATE INDEX idx_accident_time ON Accidents(accident_time);
CREATE INDEX idx_severity ON Accidents(severity);
CREATE INDEX idx_location_id ON Accidents(location_id);

WITH AccidentByHour AS (
    SELECT EXTRACT(HOUR FROM accident_time) AS hour, COUNT(*) AS accident_count
    FROM Accidents
    GROUP BY hour
)
SELECT hour, accident_count
FROM AccidentByHour
ORDER BY accident_count DESC;

SELECT accident_time,
       COUNT(*) OVER (ORDER BY accident_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_accidents
FROM Accidents
ORDER BY accident_time;

CREATE VIEW AccidentSeverityDistribution AS
SELECT YEAR(accident_time) AS year, severity, COUNT(*) AS count
FROM Accidents
GROUP BY year, severity
ORDER BY year DESC, count DESC;

DELIMITER //

CREATE PROCEDURE GetAccidentReport(IN severity_input VARCHAR(50), IN start_date DATE, IN end_date DATE)
BEGIN
    SELECT location_id, COUNT(*) AS count
    FROM Accidents
    WHERE severity = severity_input
    AND accident_time BETWEEN start_date AND end_date
    GROUP BY location_id
    ORDER BY count DESC;
END //

DELIMITER ;

CALL GetAccidentReport('Severe', '2023-01-01', '2024-12-31');

