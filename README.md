# OneSignal KBC extractor component
Export User or Notification data from OneSignal. Download data from the following API endpoints:

- [Players](https://documentation.onesignal.com/reference#csv-export) (also known as devices or users)
- [Notifications](https://documentation.onesignal.com/reference#view-notifications)


## Configuration

- **API token** - Your Onesignal app API token.
- **App ID** - Onesignal application ID. 
- **Period from date** - Date in YYYY-MM-DD format or dateparser string i.e. 5 days ago, 1 month ago, yesterday, etc. Maybe left empty for unbounded interval. This affects only the Players dataset.
- **Download also notifications dataset** - set to true if you'd like to download the notifications.


## Result

### Players
Generate players table. 

Returned columns:
- id
- identifier
- session_count
- language
- timezone
- game_version
- device_os
- device_type
- device_model
- ad_id
- tags
- last_active
- playtime
- amount_spent
- created_at
- invalid_identifier
- badge_count
- location
- country
- rooted
- notification_types
- as_id
- ip
- external_user_id
- web_auth
- web_p256

**NOTE:** Only one concurrent export is allowed per OneSignal account. Please ensure you have successfully downloaded the .csv.gz file before exporting another app.

### Notifications
Consists of several datasets linked via `notification_id`:

- notifications
- notification_filters
- notifications_excluded_segments
- notifications_include_player_ids
- notifications_included_segment
- notification_data
- headings
- platform_delivery_stats
- spoken_text


**NOTE:** that some fields contain JSON values, due to irregularity of its structure. These fields may be additionaly parsed in a Snowflake transformation.
Such fields are:

- adm_group_message
- android_group_message
- contents
 
## Development

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path:
```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository, init the workspace and run the component with following command:
```
git clone https://bitbucket.org:kds_consulting_team/kbc-python-template.git my-new-component
cd my-new-component
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint check using this command:

```
docker-compose run --rm test
```

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 