from django.conf import settings
from rest_framework.pagination import PageNumberPagination


class PartnerLeadPagination(PageNumberPagination):
    page_size = settings.PARTNER_LEADS_PAGE_SIZE
    page_size_query_param = "page_size"
    max_page_size = settings.PARTNER_LEADS_MAX_PAGE_SIZE
