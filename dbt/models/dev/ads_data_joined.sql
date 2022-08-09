{{ config(materialized='table') }}

select
    briefing.campaign_id,
    briefing.campaign_name,
    briefing.submission_date,
    briefing.description,
    briefing.campaign_objectives,
    briefing.kpis,
    briefing.placements,
    briefing.start_date,
    briefing.end_date,
    briefing.serving_locations,
    briefing.black_white_audience_list_included,
    briefing.delivery_requirements,
    briefing.cost_centre,
    briefing.currency,
    briefing.buy_rate,
    briefing.volume_agreed,
    briefing.gross_cost,
    briefing.agency_fee,
    briefing.percentage,
    briefing.flat_fee,
    briefing.net_cost,
    campaigns_inventory.type,
    campaigns_inventory.width,
    campaigns_inventory.height,
    campaigns_inventory.creative_id,
    campaigns_inventory.auction_id,
    campaigns_inventory.browser_ts,
    campaigns_inventory.game_key,
    campaigns_inventory.geo_country,
    campaigns_inventory.site_name,
    campaigns_inventory.platform_os,
    campaigns_inventory.device_type,
    campaigns_inventory.browser,
    global_design.labels,
    global_design.text,
    global_design.colors,
    global_design.video_data,
    global_design.eng_type,
    global_design.direction,
    global_design.adunit_size

from (({{ ref('briefing') }}
    full outer join {{ ref('campaigns_inventory') }}
    ON briefing.campaign_id = campaigns_inventory.campaign_id)
    full outer join {{ ref('global_design') }}
    ON global_design.game_key = campaigns_inventory.game_key)
