## Result

### Players
Generate players table. 

**NOTE:** Only one concurrent export is allowed per OneSignal account. Please ensure you have successfully downloaded the .csv.gz file before exporting another app.



**Returned columns:**

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