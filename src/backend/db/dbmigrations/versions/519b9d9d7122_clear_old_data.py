"""clear old data

Revision ID: 819b9d9d7121
Revises:
Create Date: 2021-07-13 14:58:10.902027

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '519b9d9d7122'
down_revision = '3df99e9174f4'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('DELETE FROM mirrors')
    op.execute('DELETE FROM mirrors_subnets')
    op.execute('DELETE FROM mirrors_subnets_int')
    op.execute('DELETE FROM mirrors_urls')
    op.execute('DELETE FROM subnets')
    op.execute('DELETE FROM subnets_int')
    op.execute('DELETE FROM urls')


def downgrade():
    pass
