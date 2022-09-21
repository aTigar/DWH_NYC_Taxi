```
create table taxi_facts
(
    [index]       INT,
    dateID        INT,
    locationID    INT,
    pickups       INT,
    dropoffs      INT,
    trip_distance FLOAT,
    taxiTypeID    VARCHAR(max),
    WID           INT,
    CID           INT
)
go

create table covid
(
    [index]                         INT,
    CID                             INT,
    date_of_interest                VARCHAR(max),
    case_count                      INT,
    probable_case_count             INT,
    hospitalized_count              INT,
    death_count                     INT,
    probable_death_count            INT,
    case_count_7day_avg             INT,
    all_case_count_7day_avg         INT,
    hosp_count_7day_avg             INT,
    death_count_7day_avg            INT,
    all_death_count_7day_avg        INT,
    bx_case_count                   INT,
    bx_probable_case_count          INT,
    bx_hospitalized_count           INT,
    bx_death_count                  INT,
    bx_probable_death_count         INT,
    bx_case_count_7day_avg          INT,
    bx_probable_case_count_7day_avg INT,
    bx_all_case_count_7day_avg      INT,
    bx_hospitalized_count_7day_avg  INT,
    bx_death_count_7day_avg         INT,
    bx_all_death_count_7day_avg     INT,
    bk_case_count                   INT,
    bk_probable_case_count          INT,
    bk_hospitalized_count           INT,
    bk_death_count                  INT,
    bk_probable_death_count         INT,
    bk_case_count_7day_avg          INT,
    bk_probable_case_count_7day_avg INT,
    bk_all_case_count_7day_avg      INT,
    bk_hospitalized_count_7day_avg  INT,
    bk_death_count_7day_avg         INT,
    bk_all_death_count_7day_avg     INT,
    mn_case_count                   INT,
    mn_probable_case_count          INT,
    mn_hospitalized_count           INT,
    mn_death_count                  INT,
    mn_probable_death_count         INT,
    mn_case_count_7day_avg          INT,
    mn_probable_case_count_7day_avg INT,
    mn_all_case_count_7day_avg      INT,
    mn_hospitalized_count_7day_avg  INT,
    mn_death_count_7day_avg         INT,
    mn_all_death_count_7day_avg     INT,
    qn_case_count                   INT,
    qn_probable_case_count          INT,
    qn_hospitalized_count           INT,
    qn_death_count                  INT,
    qn_probable_death_count         INT,
    qn_case_count_7day_avg          INT,
    qn_probable_case_count_7day_avg INT,
    qn_all_case_count_7day_avg      INT,
    qn_hospitalized_count_7day_avg  INT,
    qn_death_count_7day_avg         INT,
    qn_all_death_count_7day_avg     INT,
    si_case_count                   INT,
    si_probable_case_count          INT,
    si_hospitalized_count           INT,
    si_death_count                  INT,
    si_probable_death_count         INT,
    si_probable_case_count_7day_avg INT,
    si_case_count_7day_avg          INT,
    si_all_case_count_7day_avg      INT,
    si_hospitalized_count_7day_avg  INT,
    si_death_count_7day_avg         INT,
    si_all_death_count_7day_avg     INT,
    incomplete                      INT,
    dateID                          INT
)
go


create table weather
(
    [index]   INT,
    WID       INT,
    STATION   VARCHAR(max),
    NAME      VARCHAR(max),
    DATE      VARCHAR(max),
    AWND      FLOAT,
    DAPR      VARCHAR(max),
    MDPR      VARCHAR(max),
    PGTM      VARCHAR(max),
    PRCP      FLOAT,
    PSUN      VARCHAR(max),
    SNOW      FLOAT,
    SNWD      FLOAT,
    TAVG      FLOAT,
    TMAX      FLOAT,
    TMIN      FLOAT,
    TOBS      VARCHAR(max),
    TSUN      VARCHAR(max),
    WDF2      FLOAT,
    WDF5      FLOAT,
    WDFG      VARCHAR(max),
    WESD      VARCHAR(max),
    WESF      VARCHAR(max),
    WSF2      FLOAT,
    WSF5      FLOAT,
    WSFG      VARCHAR(max),
    FOG       FLOAT,
    HFOG      FLOAT,
    THUNDER   FLOAT,
    ICEP      FLOAT,
    HAIL      VARCHAR(max),
    GLAZE     FLOAT,
    DUST      VARCHAR(max),
    SMOKE     FLOAT,
    BLOW      FLOAT,
    HWIND     VARCHAR(max),
    LATITUDE  VARCHAR(max),
    LONGITUDE VARCHAR(max),
    ELEVATION VARCHAR(max),
    dateID    INT
)
go

create table calendar
(
    [index]   INT,
    date      VARCHAR(max),
    dateID    INT,
    year      INT,
    month     INT,
    day       INT,
    dayofweek INT,
    day_name  VARCHAR(max)
)
go

create table taxi_location
(
    LocationID   INT,
    Borough      VARCHAR(max),
    Zone         VARCHAR(max),
    service_zone VARCHAR(max),
    location     VARCHAR(max),
    geocode      VARCHAR(max),
    lat          FLOAT,
    long         FLOAT
)
go

```
