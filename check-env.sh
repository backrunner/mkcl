#!/bin/bash

# 设置颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo "Checking environment dependencies..."

# 检查是否有sudo权限
check_sudo() {
    if command -v sudo &> /dev/null; then
        echo -e "${GREEN}✓ Sudo is available${NC}"
        return 0
    else
        echo -e "${YELLOW}! Sudo is not available, will try to install without sudo${NC}"
        return 1
    fi
}

# 检查libpq是否安装
check_libpq() {
    if ldconfig -p | grep -q libpq; then
        echo -e "${GREEN}✓ libpq is already installed${NC}"
        return 0
    else
        echo -e "${RED}✗ libpq is not installed${NC}"
        return 1
    fi
}

# 检查screen是否安装
check_screen() {
    if command -v screen &> /dev/null; then
        echo -e "${GREEN}✓ screen is already installed${NC}"
        return 0
    else
        echo -e "${RED}✗ screen is not installed${NC}"
        return 1
    fi
}

# 安装依赖
install_dependencies() {
    echo "Installing required dependencies..."

    HAS_SUDO=$(check_sudo && echo "true" || echo "false")

    if [ "$HAS_SUDO" = "true" ]; then
        INSTALL_CMD="sudo apt-get -y install"
    else
        INSTALL_CMD="apt-get -y install"
    fi

    # 更新软件包列表
    echo "Updating package lists..."
    if [ "$HAS_SUDO" = "true" ]; then
        sudo apt-get update
    else
        apt-get update
    fi

    # 安装libpq-dev
    if ! check_libpq; then
        echo "Installing libpq-dev..."
        $INSTALL_CMD libpq-dev
        if check_libpq; then
            echo -e "${GREEN}✓ libpq-dev has been successfully installed${NC}"
        else
            echo -e "${RED}✗ Failed to install libpq-dev${NC}"
        fi
    fi

    # 安装screen
    if ! check_screen; then
        echo "Installing screen..."
        $INSTALL_CMD screen
        if check_screen; then
            echo -e "${GREEN}✓ screen has been successfully installed${NC}"
        else
            echo -e "${RED}✗ Failed to install screen${NC}"
        fi
    fi
}

# 主函数
main() {
    local libpq_installed=$(check_libpq && echo "true" || echo "false")
    local screen_installed=$(check_screen && echo "true" || echo "false")

    if [ "$libpq_installed" = "false" ] || [ "$screen_installed" = "false" ]; then
        echo -e "${YELLOW}Some dependencies are missing. Attempting to install...${NC}"
        install_dependencies
    else
        echo -e "${GREEN}All required dependencies are installed!${NC}"
    fi
}

# 执行主函数
main
